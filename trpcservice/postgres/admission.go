package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Admit atomically revalidates the trusted request identity, fixes the active
// config version, allocates a session turn, and inserts the execution and its
// transactional dispatch outbox record.
// The transaction commit is the request admission linearization point.
func (s *Store) Admit(
	ctx context.Context,
	request gateway.AdmissionRequest,
) (gateway.AdmissionResult, error) {
	if err := s.validate(); err != nil {
		return gateway.AdmissionResult{}, err
	}
	if err := request.Validate(); err != nil {
		return gateway.AdmissionResult{}, err
	}
	if request.Identity.Source != gateway.TenantSourceAuthenticatedClaims &&
		request.Identity.Source != gateway.TenantSourceVerifiedChannelBinding {
		return gateway.AdmissionResult{}, gateway.ErrUnsupportedAdmissionSource
	}
	if request.Identity.Source == gateway.TenantSourceVerifiedChannelBinding &&
		request.ChannelInput == nil {
		return gateway.AdmissionResult{}, gateway.ErrChannelInputRequired
	}
	if request.ChannelInput != nil {
		// Channel webhook idempotency is defined by the binding-scoped provider
		// message ID. Do not let a caller-supplied key alias another message.
		request.IdempotencyKey = request.ChannelInput.ExternalMessageID
	}
	var command []byte
	var payloadHash [sha256.Size]byte
	var err error
	if request.ChannelInput == nil {
		command, payloadHash, err = marshalAdmissionCommand(request.Identity.Tenant, request.Message)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("begin admission: %w", err)
	}
	defer func() {
		rollback(tx)
	}()

	var credential auth.Credential
	tenantID := request.Identity.Tenant.TenantID
	appID := request.Identity.Tenant.AppID
	if request.Identity.Source == gateway.TenantSourceAuthenticatedClaims {
		credential, err = lockCredential(ctx, tx, request.Identity.CredentialDigest)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return gateway.AdmissionResult{}, auth.ErrUnauthenticated
			}
			return gateway.AdmissionResult{}, err
		}
		if err := validateAdmissionCredential(credential, request.Identity); err != nil {
			return gateway.AdmissionResult{}, err
		}
		tenantID = credential.TenantID
		appID = credential.AppID
	} else {
		credential = auth.Credential{
			ID:       request.Identity.SourceID,
			TenantID: tenantID,
			AppID:    appID,
			Status:   auth.CredentialActive,
		}
	}

	tnt, err := lockTenant(ctx, tx, tenantID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return gateway.AdmissionResult{}, auth.ErrUnauthenticated
		}
		return gateway.AdmissionResult{}, err
	}
	if tnt.Status != tenant.StatusActive {
		return gateway.AdmissionResult{}, auth.ErrTenantInactive
	}
	if tnt.ID != request.Identity.Tenant.TenantID {
		return gateway.AdmissionResult{}, auth.ErrUnauthenticated
	}

	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return gateway.AdmissionResult{}, auth.ErrUnauthenticated
		}
		return gateway.AdmissionResult{}, err
	}
	if app.Status != tenant.StatusActive {
		return gateway.AdmissionResult{}, auth.ErrAppInactive
	}
	if app.TenantID != request.Identity.Tenant.TenantID ||
		app.AppID != request.Identity.Tenant.AppID {
		return gateway.AdmissionResult{}, auth.ErrUnauthenticated
	}
	if request.Identity.Source == gateway.TenantSourceVerifiedChannelBinding {
		if err := revalidateChannelBindingSnapshot(ctx, tx, request.Identity); err != nil {
			return gateway.AdmissionResult{}, err
		}
	}
	if _, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, app.ActiveConfigVersion); err != nil {
		return gateway.AdmissionResult{}, err
	}
	admission := admissionTransaction{
		ctx:              ctx,
		tx:               tx,
		request:          request,
		credential:       credential,
		app:              app,
		command:          command,
		payloadHash:      payloadHash,
		runtimeContext:   request.Identity.Tenant,
		channelAdmission: request.ChannelInput != nil,
	}
	if request.ChannelInput != nil {
		if s.identityMapper == nil {
			return gateway.AdmissionResult{}, errors.New("channel identity mapper is required")
		}
		mappingRequest, err := channelIdentityMappingRequest(request)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		payloadHash, err = s.identityMapper.channelPayloadHash(ctx, mappingRequest, *request.ChannelInput)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		admission.payloadHash = payloadHash
		inbox, found, err := findChannelInbox(
			ctx,
			tx,
			request.Identity.Tenant.TenantID,
			request.Identity.Tenant.AppID,
			request.Identity.Tenant.BindingID,
			request.ChannelInput.ExternalMessageID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if found {
			return admission.reconcileChannelInbox(inbox)
		}
		rejectReason, err := channelInputRejectReason(*request.ChannelInput)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if rejectReason != "" {
			if err := insertChannelInbox(
				ctx,
				tx,
				request,
				payloadHash[:],
				channelInboxStatusRejected,
				rejectReason,
				nil,
				nil,
			); err != nil {
				return gateway.AdmissionResult{}, admissionInsertError("insert rejected channel inbox", err)
			}
			if result, replayed, err := admission.reconcileInsertedChannelInbox(); err != nil {
				return gateway.AdmissionResult{}, err
			} else if replayed {
				return result, nil
			}
			if err := tx.Commit(ctx); err != nil {
				return gateway.AdmissionResult{}, fmt.Errorf("commit rejected channel admission: %w", err)
			}
			return gateway.AdmissionResult{
				RequestID: request.RequestID,
				Status:    gateway.AdmissionStatusRejected,
			}, nil
		}
		targetEnvelope, targetExpiresAt, err := request.ChannelInput.SealMessageReplyTarget(
			ctx,
			s.identityMapper.protector,
			request.Identity.Tenant.Scope(),
			request.RequestID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if err := insertChannelInbox(
			ctx,
			tx,
			request,
			payloadHash[:],
			channelInboxStatusAdmitted,
			"",
			targetEnvelope,
			targetExpiresAt,
		); err != nil {
			return gateway.AdmissionResult{}, admissionInsertError("insert channel inbox", err)
		}
		if result, replayed, err := admission.reconcileInsertedChannelInbox(); err != nil {
			return gateway.AdmissionResult{}, err
		} else if replayed {
			return result, nil
		}
		mapped, err := s.identityMapper.mapInTransaction(ctx, tx, mappingRequest)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		admission.runtimeContext = request.Identity.Tenant
		admission.runtimeContext.ConfigVersion = app.ActiveConfigVersion
		admission.runtimeContext.BindingRevision = request.Identity.BindingRevision
		admission.runtimeContext.SessionPrincipalID = mapped.SessionPrincipalID
		admission.runtimeContext.SessionID = mapped.SessionID
		admission.runtimeContext.UserID = mapped.Identity.UserID
		admission.command, _, err = marshalAdmissionCommand(
			admission.runtimeContext,
			request.Message,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
	}
	existing, found, err := findExecutionByIdempotency(
		ctx,
		tx,
		idempotencyLookup{
			TenantID:       credential.TenantID,
			AppID:          credential.AppID,
			Source:         request.Identity.Source,
			SourceID:       request.Identity.SourceID,
			IdempotencyKey: request.IdempotencyKey,
		},
	)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if found {
		return admission.reconcileExisting(existing)
	}
	return admission.createExecution()
}

type admissionTransaction struct {
	ctx              context.Context
	tx               pgx.Tx
	request          gateway.AdmissionRequest
	credential       auth.Credential
	app              tenant.AgentApp
	command          []byte
	payloadHash      [sha256.Size]byte
	runtimeContext   tenant.RuntimeContext
	channelAdmission bool
}

func channelIdentityMappingRequest(request gateway.AdmissionRequest) (IdentityMappingRequest, error) {
	if request.ChannelInput == nil {
		return IdentityMappingRequest{}, errors.New("channel input is required")
	}
	mapping, ok := request.ChannelInput.MappingInput()
	if !ok {
		return IdentityMappingRequest{}, errors.New("channel mapping input is required")
	}
	return IdentityMappingRequest{
		Scope:                      request.Identity.Tenant.Scope(),
		BindingID:                  request.Identity.Tenant.BindingID,
		Channel:                    channels.Channel(request.Identity.Tenant.Channel),
		Kind:                       request.ChannelInput.Conversation.Kind,
		ExternalSenderID:           mapping.ExternalSenderID,
		ExternalChatID:             mapping.ExternalChatID,
		ExternalThreadID:           mapping.ExternalThreadID,
		ProviderSenderTarget:       mapping.ProviderSenderTarget,
		ProviderConversationTarget: mapping.ProviderConversationTarget,
		ProviderThreadTarget:       mapping.ProviderThreadTarget,
	}, nil
}

func (a admissionTransaction) reconcileChannelInbox(
	inbox channelInboxRecord,
) (gateway.AdmissionResult, error) {
	if !bytes.Equal(inbox.PayloadHash, a.payloadHash[:]) {
		return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
	}
	switch inbox.Status {
	case channelInboxStatusRejected:
		if err := a.tx.Commit(a.ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit rejected channel replay: %w", err)
		}
		return gateway.AdmissionResult{
			RequestID: inbox.RequestID,
			Replayed:  true,
			Status:    gateway.AdmissionStatusRejected,
		}, nil
	case channelInboxStatusAdmitted:
		existing, found, err := findExecutionByRequestID(
			a.ctx,
			a.tx,
			a.credential.TenantID,
			a.credential.AppID,
			inbox.RequestID,
		)
		if err != nil {
			return gateway.AdmissionResult{}, err
		}
		if !found {
			return gateway.AdmissionResult{}, errors.New("admitted channel inbox has no execution")
		}
		if !bytes.Equal(existing.PayloadHash, a.payloadHash[:]) {
			return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
		}
		result := gateway.AdmissionResult{
			RequestID:     existing.RequestID,
			ConfigVersion: existing.ConfigVersion,
			TurnSeq:       existing.TurnSeq,
			Replayed:      true,
			Status:        gateway.AdmissionStatusAdmitted,
		}
		if err := a.tx.Commit(a.ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit admitted channel replay: %w", err)
		}
		return result, nil
	default:
		return gateway.AdmissionResult{}, fmt.Errorf("channel inbox status %q is invalid", inbox.Status)
	}
}

func (a admissionTransaction) reconcileInsertedChannelInbox() (gateway.AdmissionResult, bool, error) {
	input := a.request.ChannelInput
	if input == nil {
		return gateway.AdmissionResult{}, false, errors.New("channel input is required")
	}
	inbox, found, err := findChannelInbox(
		a.ctx,
		a.tx,
		input.TenantID,
		input.AppID,
		input.BindingID,
		input.ExternalMessageID,
	)
	if err != nil {
		return gateway.AdmissionResult{}, false, err
	}
	if !found {
		return gateway.AdmissionResult{}, false, errors.New("channel inbox was not available after insert")
	}
	if !bytes.Equal(inbox.PayloadHash, a.payloadHash[:]) {
		return gateway.AdmissionResult{}, false, gateway.ErrIdempotencyConflict
	}
	if inbox.RequestID == a.request.RequestID {
		return gateway.AdmissionResult{}, false, nil
	}
	result, err := a.reconcileChannelInbox(inbox)
	return result, true, err
}

func (a admissionTransaction) reconcileExisting(existing admissionExecution) (gateway.AdmissionResult, error) {
	if !bytes.Equal(existing.PayloadHash, a.payloadHash[:]) {
		return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
	}
	if existing.Status != "FAILED" {
		status := gateway.AdmissionStatus("")
		if a.channelAdmission {
			status = gateway.AdmissionStatusAdmitted
		}
		result := gateway.AdmissionResult{
			RequestID:     existing.RequestID,
			ConfigVersion: existing.ConfigVersion,
			TurnSeq:       existing.TurnSeq,
			Replayed:      true,
			Status:        status,
		}
		if err := a.tx.Commit(a.ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit replayed admission: %w", err)
		}
		return result, nil
	}
	if a.channelAdmission {
		result := gateway.AdmissionResult{
			RequestID:     existing.RequestID,
			ConfigVersion: existing.ConfigVersion,
			TurnSeq:       existing.TurnSeq,
			Replayed:      true,
			Status:        gateway.AdmissionStatusAdmitted,
		}
		if err := a.tx.Commit(a.ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit failed channel replay: %w", err)
		}
		return result, nil
	}
	blocked, err := migrationBlocksAdmission(a.ctx, a.tx, a.app.TenantID, a.app.AppID)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if blocked {
		return gateway.AdmissionResult{}, gateway.ErrAdmissionDraining
	}
	// A failed terminal execution is re-armed instead of replayed so
	// clients can retry the same logical request. The retry runs with
	// the currently active configuration and a fresh attempt budget;
	// the relay recovery refills its dispatch record.
	tag, err := a.tx.Exec(
		a.ctx,
		`UPDATE platform.execution
SET status = 'PENDING', attempt = 0, config_version = $4, last_error = NULL,
    lease_owner = NULL, run_token = NULL, lease_until = NULL, finished_at = NULL,
    next_attempt_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND status = 'FAILED'`,
		a.credential.TenantID,
		a.credential.AppID,
		existing.RequestID,
		a.app.ActiveConfigVersion,
	)
	if err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("re-arm failed execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return gateway.AdmissionResult{}, fmt.Errorf("re-arm failed execution: %w", ErrNotFound)
	}
	if err := a.tx.Commit(a.ctx); err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("commit re-armed admission: %w", err)
	}
	return gateway.AdmissionResult{
		RequestID:     existing.RequestID,
		ConfigVersion: a.app.ActiveConfigVersion,
		TurnSeq:       existing.TurnSeq,
	}, nil
}

func (a admissionTransaction) createExecution() (gateway.AdmissionResult, error) {
	blocked, err := migrationBlocksAdmission(a.ctx, a.tx, a.app.TenantID, a.app.AppID)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if blocked {
		return gateway.AdmissionResult{}, gateway.ErrAdmissionDraining
	}
	requestExists, err := executionRequestExists(a.ctx, a.tx, a.credential.TenantID, a.credential.AppID, a.request.RequestID)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if requestExists {
		return gateway.AdmissionResult{}, fmt.Errorf("request_id already exists: %w", gateway.ErrIdempotencyConflict)
	}

	runtimeContext := a.runtimeContext
	if runtimeContext == (tenant.RuntimeContext{}) {
		runtimeContext = a.request.Identity.Tenant
	}
	turnSeq, err := allocateSessionTurn(
		a.ctx,
		a.tx,
		a.credential.TenantID,
		a.credential.AppID,
		runtimeContext.SessionPrincipalID,
		runtimeContext.SessionID,
	)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if a.request.ChannelInput != nil {
		if err := attachStagedInboundArtifacts(
			a.ctx,
			a.tx,
			*a.request.ChannelInput,
			a.app.ActiveConfigVersion,
			runtimeContext.SessionPrincipalID,
			runtimeContext.SessionID,
		); err != nil {
			return gateway.AdmissionResult{}, err
		}
	}
	if _, err := a.tx.Exec(
		a.ctx,
		`INSERT INTO platform.execution (
    tenant_id,
    app_id,
    request_id,
    session_principal_id,
    session_id,
    user_id,
    turn_seq,
    config_version,
    tenant_source,
    source_id,
    idempotency_key,
    payload_hash,
    command,
    status,
    trace_id
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'PENDING', $14)`,
		a.credential.TenantID,
		a.credential.AppID,
		a.request.RequestID,
		runtimeContext.SessionPrincipalID,
		runtimeContext.SessionID,
		runtimeContext.UserID,
		turnSeq,
		a.app.ActiveConfigVersion,
		a.request.Identity.Source,
		a.request.Identity.SourceID,
		a.request.IdempotencyKey,
		a.payloadHash[:],
		a.command,
		a.request.Identity.Tenant.TraceID,
	); err != nil {
		return gateway.AdmissionResult{}, admissionInsertError("insert execution", err)
	}
	if _, err := a.tx.Exec(
		a.ctx,
		`INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id)
VALUES ($1, $2, $3)`,
		a.credential.TenantID,
		a.credential.AppID,
		a.request.RequestID,
	); err != nil {
		return gateway.AdmissionResult{}, admissionInsertError("insert dispatch outbox", err)
	}

	if err := a.tx.Commit(a.ctx); err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("commit admission: %w", err)
	}
	return gateway.AdmissionResult{
		RequestID:     a.request.RequestID,
		ConfigVersion: a.app.ActiveConfigVersion,
		TurnSeq:       turnSeq,
		Status:        gateway.AdmissionStatusAdmitted,
	}, nil
}

type databaseQueryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func lockCredential(
	ctx context.Context,
	tx pgx.Tx,
	digest gateway.CredentialDigest,
) (auth.Credential, error) {
	var credential auth.Credential
	var expiresAt *time.Time
	err := tx.QueryRow(
		ctx,
		`SELECT credential_id, tenant_id, app_id, key_prefix, status, expires_at
FROM platform.api_credential
WHERE key_digest = $1
FOR UPDATE`,
		digest[:],
	).Scan(
		&credential.ID,
		&credential.TenantID,
		&credential.AppID,
		&credential.KeyPrefix,
		&credential.Status,
		&expiresAt,
	)
	if err != nil {
		return auth.Credential{}, resolveError("api credential", err)
	}
	if expiresAt != nil {
		credential.ExpiresAt = expiresAt.UTC()
	}
	return credential, nil
}

func validateAdmissionCredential(
	credential auth.Credential,
	identity gateway.AdmissionIdentity,
) error {
	if err := credential.Validate(); err != nil {
		return fmt.Errorf("stored api credential: %w", err)
	}
	if credential.Status != auth.CredentialActive ||
		(!credential.ExpiresAt.IsZero() && !time.Now().Before(credential.ExpiresAt)) {
		return auth.ErrCredentialInactive
	}
	if credential.ID != identity.SourceID ||
		credential.TenantID != identity.Tenant.TenantID ||
		credential.AppID != identity.Tenant.AppID {
		return auth.ErrUnauthenticated
	}
	return nil
}

func lockTenant(ctx context.Context, tx pgx.Tx, tenantID string) (tenant.Tenant, error) {
	var value tenant.Tenant
	err := tx.QueryRow(
		ctx,
		`SELECT tenant_id, name, status
FROM platform.tenant
WHERE tenant_id = $1
FOR UPDATE`,
		tenantID,
	).Scan(&value.ID, &value.Name, &value.Status)
	if err != nil {
		return tenant.Tenant{}, resolveError("tenant", err)
	}
	if err := value.Validate(); err != nil {
		return tenant.Tenant{}, fmt.Errorf("stored tenant: %w", err)
	}
	return value, nil
}

func lockAgentApp(ctx context.Context, tx pgx.Tx, tenantID, appID string) (tenant.AgentApp, error) {
	var app tenant.AgentApp
	err := tx.QueryRow(
		ctx,
		`SELECT tenant_id, app_id, name, active_config_version, status
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE`,
		tenantID,
		appID,
	).Scan(
		&app.TenantID,
		&app.AppID,
		&app.Name,
		&app.ActiveConfigVersion,
		&app.Status,
	)
	if err != nil {
		return tenant.AgentApp{}, resolveError("agent app", err)
	}
	if err := app.Validate(); err != nil {
		return tenant.AgentApp{}, fmt.Errorf("stored agent app: %w", err)
	}
	return app, nil
}

func revalidateChannelBindingSnapshot(
	ctx context.Context,
	tx pgx.Tx,
	identity gateway.AdmissionIdentity,
) error {
	binding, err := lockChannelBinding(
		ctx,
		tx,
		identity.Tenant.TenantID,
		identity.Tenant.AppID,
		identity.Tenant.BindingID,
	)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return auth.ErrUnauthenticated
		}
		return err
	}
	if binding.Status != channels.BindingActive {
		return channels.ErrBindingInactive
	}
	if binding.TenantID != identity.Tenant.TenantID ||
		binding.AppID != identity.Tenant.AppID ||
		binding.BindingID != identity.Tenant.BindingID ||
		binding.BindingID != identity.SourceID ||
		binding.Channel != channels.Channel(identity.Tenant.Channel) {
		return gateway.ErrChannelBindingSnapshotStale
	}
	if binding.PublicRouteID != identity.PublicRouteID ||
		binding.BindingRevision != identity.BindingRevision {
		return gateway.ErrChannelBindingSnapshotStale
	}
	return nil
}

func resolveAppConfigFrom(
	ctx context.Context,
	db databaseQueryer,
	tenantID, appID, version string,
) (tenant.AppConfig, error) {
	var encoded appConfigColumns
	err := db.QueryRow(
		ctx,
		`SELECT model_config, tool_policy, backend_config,
       secret_refs, channel_binding_ids, knowledge_base_ids
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3 AND status = 'PUBLISHED'`,
		tenantID,
		appID,
		version,
	).Scan(
		&encoded.modelConfig,
		&encoded.toolPolicy,
		&encoded.backendConfig,
		&encoded.secretRefs,
		&encoded.channelBindingIDs,
		&encoded.knowledgeBaseIDs,
	)
	if err != nil {
		return tenant.AppConfig{}, resolveError("app config", err)
	}
	return unmarshalAppConfig(tenantID, appID, version, encoded)
}

type admissionExecution struct {
	RequestID     string
	ConfigVersion string
	TurnSeq       int64
	PayloadHash   []byte
	Status        string
}

type idempotencyLookup struct {
	TenantID       string
	AppID          string
	Source         gateway.TenantSource
	SourceID       string
	IdempotencyKey string
}

func findExecutionByIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	lookup idempotencyLookup,
) (admissionExecution, bool, error) {
	var value admissionExecution
	err := tx.QueryRow(
		ctx,
		`SELECT request_id, config_version, turn_seq, payload_hash, status
FROM platform.execution
WHERE tenant_id = $1
  AND app_id = $2
  AND tenant_source = $3
  AND source_id = $4
  AND idempotency_key = $5
FOR UPDATE`,
		lookup.TenantID,
		lookup.AppID,
		lookup.Source,
		lookup.SourceID,
		lookup.IdempotencyKey,
	).Scan(&value.RequestID, &value.ConfigVersion, &value.TurnSeq, &value.PayloadHash, &value.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return admissionExecution{}, false, nil
	}
	if err != nil {
		return admissionExecution{}, false, fmt.Errorf("find idempotent execution: %w", err)
	}
	return value, true, nil
}

func findExecutionByRequestID(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, requestID string,
) (admissionExecution, bool, error) {
	var value admissionExecution
	err := tx.QueryRow(
		ctx,
		`SELECT request_id, config_version, turn_seq, payload_hash, status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
FOR UPDATE`,
		tenantID,
		appID,
		requestID,
	).Scan(&value.RequestID, &value.ConfigVersion, &value.TurnSeq, &value.PayloadHash, &value.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return admissionExecution{}, false, nil
	}
	if err != nil {
		return admissionExecution{}, false, fmt.Errorf("find channel execution: %w", err)
	}
	return value, true, nil
}

func executionRequestExists(ctx context.Context, tx pgx.Tx, tenantID, appID, requestID string) (bool, error) {
	var exists bool
	err := tx.QueryRow(
		ctx,
		`SELECT EXISTS (
    SELECT 1
    FROM platform.execution
    WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
)`,
		tenantID,
		appID,
		requestID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check request id: %w", err)
	}
	return exists, nil
}

func allocateSessionTurn(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, principalID, sessionID string,
) (int64, error) {
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.session_lane (
    tenant_id, app_id, session_principal_id, session_id
) VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, app_id, session_principal_id, session_id) DO NOTHING`,
		tenantID,
		appID,
		principalID,
		sessionID,
	); err != nil {
		return 0, fmt.Errorf("create session lane: %w", err)
	}
	var nextTurnSeq int64
	if err := tx.QueryRow(
		ctx,
		`SELECT next_turn_seq
FROM platform.session_lane
WHERE tenant_id = $1
  AND app_id = $2
  AND session_principal_id = $3
  AND session_id = $4
FOR UPDATE`,
		tenantID,
		appID,
		principalID,
		sessionID,
	).Scan(&nextTurnSeq); err != nil {
		return 0, fmt.Errorf("lock session lane: %w", err)
	}
	if nextTurnSeq <= 0 {
		return 0, errors.New("session lane next_turn_seq is invalid")
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE platform.session_lane
SET next_turn_seq = next_turn_seq + 1, updated_at = now()
WHERE tenant_id = $1
  AND app_id = $2
  AND session_principal_id = $3
  AND session_id = $4`,
		tenantID,
		appID,
		principalID,
		sessionID,
	); err != nil {
		return 0, fmt.Errorf("advance session lane: %w", err)
	}
	return nextTurnSeq, nil
}

type admissionCommand struct {
	TenantID           string   `json:"tenant_id"`
	AppID              string   `json:"app_id"`
	Channel            string   `json:"channel,omitempty"`
	BindingID          string   `json:"binding_id,omitempty"`
	BindingRevision    int64    `json:"binding_revision,omitempty"`
	SessionPrincipalID string   `json:"session_principal_id"`
	SessionID          string   `json:"session_id"`
	UserID             string   `json:"user_id"`
	Text               string   `json:"text"`
	ArtifactRefs       []string `json:"artifact_refs,omitempty"`
}

func marshalAdmissionCommand(
	runtimeContext tenant.RuntimeContext,
	message gateway.Message,
) ([]byte, [sha256.Size]byte, error) {
	command, err := json.Marshal(admissionCommand{
		TenantID:           runtimeContext.TenantID,
		AppID:              runtimeContext.AppID,
		Channel:            runtimeContext.Channel,
		BindingID:          runtimeContext.BindingID,
		BindingRevision:    runtimeContext.BindingRevision,
		SessionPrincipalID: runtimeContext.SessionPrincipalID,
		SessionID:          runtimeContext.SessionID,
		UserID:             runtimeContext.UserID,
		Text:               message.Text,
		ArtifactRefs:       append([]string(nil), message.ArtifactRefs...),
	})
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("marshal admission command: %w", err)
	}
	return command, sha256.Sum256(command), nil
}

func admissionInsertError(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) &&
		(pgErr.ConstraintName == "channel_inbox_pkey" ||
			pgErr.ConstraintName == "execution_pkey" ||
			pgErr.ConstraintName == "execution_idempotency_idx") {
		return fmt.Errorf("%s: %w", operation, gateway.ErrIdempotencyConflict)
	}
	return fmt.Errorf("%s: %w", operation, err)
}
