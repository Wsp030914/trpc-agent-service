package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
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
	if request.Identity.Source != gateway.TenantSourceAuthenticatedClaims {
		return gateway.AdmissionResult{}, gateway.ErrUnsupportedAdmissionSource
	}
	command, payloadHash, err := marshalAdmissionCommand(request.Identity, request.Message)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("begin admission: %w", err)
	}
	defer func() {
		rollback(tx)
	}()

	credential, err := lockCredential(ctx, tx, request.Identity.CredentialDigest)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return gateway.AdmissionResult{}, auth.ErrUnauthenticated
		}
		return gateway.AdmissionResult{}, err
	}
	if err := validateAdmissionCredential(credential, request.Identity); err != nil {
		return gateway.AdmissionResult{}, err
	}

	tnt, err := lockTenant(ctx, tx, credential.TenantID)
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

	app, err := lockAgentApp(ctx, tx, credential.TenantID, credential.AppID)
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
	if _, err := resolveAppConfigFrom(ctx, tx, app.TenantID, app.AppID, app.ActiveConfigVersion); err != nil {
		return gateway.AdmissionResult{}, err
	}

	existing, found, err := findExecutionByIdempotency(
		ctx,
		tx,
		credential.TenantID,
		credential.AppID,
		request.Identity.Source,
		request.Identity.SourceID,
		request.IdempotencyKey,
	)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if found {
		if !bytes.Equal(existing.PayloadHash, payloadHash[:]) {
			return gateway.AdmissionResult{}, gateway.ErrIdempotencyConflict
		}
		if existing.Status == "FAILED" {
			// A failed terminal execution is re-armed instead of replayed so
			// clients can retry the same logical request. The retry runs with
			// the currently active configuration and a fresh attempt budget;
			// the relay recovery refills its dispatch record.
			tag, err := tx.Exec(
				ctx,
				`UPDATE platform.execution
SET status = 'PENDING', attempt = 0, config_version = $4, last_error = NULL,
    lease_owner = NULL, run_token = NULL, lease_until = NULL, finished_at = NULL,
    next_attempt_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND status = 'FAILED'`,
				credential.TenantID,
				credential.AppID,
				existing.RequestID,
				app.ActiveConfigVersion,
			)
			if err != nil {
				return gateway.AdmissionResult{}, fmt.Errorf("re-arm failed execution: %w", err)
			}
			if tag.RowsAffected() != 1 {
				return gateway.AdmissionResult{}, fmt.Errorf("re-arm failed execution: %w", ErrNotFound)
			}
			if err := tx.Commit(ctx); err != nil {
				return gateway.AdmissionResult{}, fmt.Errorf("commit re-armed admission: %w", err)
			}
			return gateway.AdmissionResult{
				RequestID:     existing.RequestID,
				ConfigVersion: app.ActiveConfigVersion,
				TurnSeq:       existing.TurnSeq,
			}, nil
		}
		result := gateway.AdmissionResult{
			RequestID:     existing.RequestID,
			ConfigVersion: existing.ConfigVersion,
			TurnSeq:       existing.TurnSeq,
			Replayed:      true,
		}
		if err := tx.Commit(ctx); err != nil {
			return gateway.AdmissionResult{}, fmt.Errorf("commit replayed admission: %w", err)
		}
		return result, nil
	}

	requestExists, err := executionRequestExists(ctx, tx, credential.TenantID, credential.AppID, request.RequestID)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if requestExists {
		return gateway.AdmissionResult{}, fmt.Errorf("request_id already exists: %w", gateway.ErrIdempotencyConflict)
	}

	turnSeq, err := allocateSessionTurn(
		ctx,
		tx,
		credential.TenantID,
		credential.AppID,
		request.Identity.Tenant.SessionPrincipalID,
		request.Identity.Tenant.SessionID,
	)
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	if _, err := tx.Exec(
		ctx,
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
		credential.TenantID,
		credential.AppID,
		request.RequestID,
		request.Identity.Tenant.SessionPrincipalID,
		request.Identity.Tenant.SessionID,
		request.Identity.Tenant.UserID,
		turnSeq,
		app.ActiveConfigVersion,
		request.Identity.Source,
		request.Identity.SourceID,
		request.IdempotencyKey,
		payloadHash[:],
		command,
		request.Identity.Tenant.TraceID,
	); err != nil {
		return gateway.AdmissionResult{}, admissionInsertError("insert execution", err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id)
VALUES ($1, $2, $3)`,
		credential.TenantID,
		credential.AppID,
		request.RequestID,
	); err != nil {
		return gateway.AdmissionResult{}, admissionInsertError("insert dispatch outbox", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return gateway.AdmissionResult{}, fmt.Errorf("commit admission: %w", err)
	}
	return gateway.AdmissionResult{
		RequestID:     request.RequestID,
		ConfigVersion: app.ActiveConfigVersion,
		TurnSeq:       turnSeq,
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
	var auditPolicy []byte
	err := tx.QueryRow(
		ctx,
		`SELECT tenant_id, name, status, audit_policy
FROM platform.tenant
WHERE tenant_id = $1
FOR UPDATE`,
		tenantID,
	).Scan(&value.ID, &value.Name, &value.Status, &auditPolicy)
	if err != nil {
		return tenant.Tenant{}, resolveError("tenant", err)
	}
	value.Audit, err = unmarshalAuditPolicy(auditPolicy)
	if err != nil {
		return tenant.Tenant{}, err
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

func resolveAppConfigFrom(
	ctx context.Context,
	db databaseQueryer,
	tenantID, appID, version string,
) (tenant.AppConfig, error) {
	var modelConfig []byte
	var toolPolicy []byte
	var backendConfig []byte
	var auditPolicy []byte
	var secretRefs []byte
	var channelBindingIDs []byte
	err := db.QueryRow(
		ctx,
		`SELECT model_config, tool_policy, backend_config, audit_policy,
       secret_refs, channel_binding_ids
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3 AND status = 'PUBLISHED'`,
		tenantID,
		appID,
		version,
	).Scan(
		&modelConfig,
		&toolPolicy,
		&backendConfig,
		&auditPolicy,
		&secretRefs,
		&channelBindingIDs,
	)
	if err != nil {
		return tenant.AppConfig{}, resolveError("app config", err)
	}
	return unmarshalAppConfig(
		tenantID,
		appID,
		version,
		modelConfig,
		toolPolicy,
		backendConfig,
		auditPolicy,
		secretRefs,
		channelBindingIDs,
	)
}

type admissionExecution struct {
	RequestID     string
	ConfigVersion string
	TurnSeq       int64
	PayloadHash   []byte
	Status        string
}

func findExecutionByIdempotency(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID string,
	source gateway.TenantSource,
	sourceID, idempotencyKey string,
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
		tenantID,
		appID,
		source,
		sourceID,
		idempotencyKey,
	).Scan(&value.RequestID, &value.ConfigVersion, &value.TurnSeq, &value.PayloadHash, &value.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return admissionExecution{}, false, nil
	}
	if err != nil {
		return admissionExecution{}, false, fmt.Errorf("find idempotent execution: %w", err)
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
	SessionPrincipalID string   `json:"session_principal_id"`
	SessionID          string   `json:"session_id"`
	UserID             string   `json:"user_id"`
	Text               string   `json:"text"`
	ArtifactRefs       []string `json:"artifact_refs,omitempty"`
}

func marshalAdmissionCommand(
	identity gateway.AdmissionIdentity,
	message gateway.Message,
) ([]byte, [sha256.Size]byte, error) {
	command, err := json.Marshal(admissionCommand{
		TenantID:           identity.Tenant.TenantID,
		AppID:              identity.Tenant.AppID,
		SessionPrincipalID: identity.Tenant.SessionPrincipalID,
		SessionID:          identity.Tenant.SessionID,
		UserID:             identity.Tenant.UserID,
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
		(pgErr.ConstraintName == "execution_pkey" || pgErr.ConstraintName == "execution_idempotency_idx") {
		return fmt.Errorf("%s: %w", operation, gateway.ErrIdempotencyConflict)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

const (
	maxExecutionAttempts = 3
	blockedDispatchDelay = time.Second
	defaultDispatchBatch = 32
	// consumedOutboxRetention bounds how long consumed dispatch records are
	// kept for observability before RecoverDispatches removes them.
	consumedOutboxRetention = 24 * time.Hour
)

// Claim changes one dispatched execution into a worker-owned run. It retains
// PostgreSQL session-lane order even when Redis delivers entries out of order.
func (s *Store) Claim(ctx context.Context, dispatch queue.Dispatch, request queue.ClaimRequest) (queue.Claim, bool, error) {
	if err := s.validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := dispatch.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := request.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return queue.Claim{}, false, fmt.Errorf("begin execution claim: %w", err)
	}
	defer func() { rollback(tx) }()
	stored, active, err := lockExecutionForClaim(ctx, tx, dispatch)
	if err != nil {
		return queue.Claim{}, false, err
	}
	if !active || stored.status == "SUCCEEDED" || stored.status == "FAILED" {
		if err := consumeDispatch(ctx, tx, dispatch.OutboxID); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit unavailable execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if stored.status == "RUNNING" && stored.leaseUntil.After(time.Now()) {
		if err := consumeDispatch(ctx, tx, dispatch.OutboxID); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit duplicate execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if stored.nextAttemptAt.After(time.Now()) || stored.hasEarlier {
		if err := deferDispatch(ctx, tx, dispatch, stored.nextAttemptAt); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit deferred execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	var leaseUntil time.Time
	token := uuid.NewString()
	err = tx.QueryRow(ctx, `UPDATE platform.execution
SET status = 'RUNNING', attempt = attempt + 1, lease_owner = $4, run_token = $5,
    lease_until = clock_timestamp() + $6::interval, next_attempt_at = clock_timestamp(),
    started_at = COALESCE(started_at, clock_timestamp()), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
RETURNING lease_until, attempt`, stored.tenantID, stored.appID, stored.requestID, request.Owner, token, intervalLiteral(request.LeaseDuration)).Scan(&leaseUntil, &stored.attempt)
	if err != nil {
		return queue.Claim{}, false, fmt.Errorf("claim execution: %w", err)
	}
	if err := consumeDispatch(ctx, tx, dispatch.OutboxID); err != nil {
		return queue.Claim{}, false, err
	}
	job, err := stored.executionJob()
	if err != nil {
		return queue.Claim{}, false, err
	}
	claim := queue.Claim{Job: job, TurnSeq: stored.turnSeq, Attempt: stored.attempt, Lease: queue.Lease{Owner: request.Owner, Token: token, Until: leaseUntil.UTC()}}
	if err := claim.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return queue.Claim{}, false, fmt.Errorf("commit execution claim: %w", err)
	}
	return claim, true, nil
}

// Renew extends one active run token. A lost or expired claim cannot be renewed.
func (s *Store) Renew(ctx context.Context, claim queue.Claim, duration time.Duration) (queue.Lease, error) {
	if err := s.validate(); err != nil {
		return queue.Lease{}, err
	}
	if err := claim.Validate(); err != nil {
		return queue.Lease{}, err
	}
	if duration <= 0 {
		return queue.Lease{}, errors.New("lease duration must be positive")
	}
	var until time.Time
	err := s.pool.QueryRow(ctx, `UPDATE platform.execution SET lease_until = clock_timestamp() + $6::interval, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()
RETURNING lease_until`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token, intervalLiteral(duration)).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return queue.Lease{}, fmt.Errorf("renew execution: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return queue.Lease{}, fmt.Errorf("renew execution: %w", err)
	}
	return queue.Lease{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Until: until.UTC()}, nil
}

// Complete records a terminal result only when the caller still owns its run token.
func (s *Store) Complete(ctx context.Context, claim queue.Claim, status queue.CompletionStatus) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.execution SET status = $6, lease_owner = NULL, run_token = NULL,
lease_until = NULL, finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token, status)
	if err != nil {
		return fmt.Errorf("complete execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete execution: %w", queue.ErrLeaseLost)
	}
	return nil
}

// Retry returns a failed run to PENDING and creates its next dispatch in the
// same transaction. It terminally fails after a bounded retry count.
func (s *Store) Retry(ctx context.Context, claim queue.Claim, cause error) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin execution retry: %w", err)
	}
	defer func() { rollback(tx) }()
	var attempt int
	err = tx.QueryRow(ctx, `SELECT attempt FROM platform.execution WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3 AND status='RUNNING' AND lease_owner=$4 AND run_token=$5 AND lease_until > clock_timestamp() FOR UPDATE`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("retry execution: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock execution retry: %w", err)
	}
	if attempt >= maxExecutionAttempts {
		_, err = tx.Exec(ctx, `UPDATE platform.execution SET status='FAILED', last_error=$4, lease_owner=NULL, run_token=NULL, lease_until=NULL, finished_at=clock_timestamp(), updated_at=clock_timestamp() WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), truncateExecutionError(cause))
	} else {
		delay := time.Duration(attempt) * time.Second
		_, err = tx.Exec(ctx, `UPDATE platform.execution SET status='PENDING', last_error=$4, lease_owner=NULL, run_token=NULL, lease_until=NULL, next_attempt_at=clock_timestamp()+$5::interval, updated_at=clock_timestamp() WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), truncateExecutionError(cause), intervalLiteral(delay))
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id, next_attempt_at) VALUES ($1,$2,$3,clock_timestamp()+$4::interval)`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), intervalLiteral(delay))
		}
	}
	if err != nil {
		return fmt.Errorf("retry execution: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution retry: %w", err)
	}
	return nil
}

// ClaimDispatches leases pending outbox rows for a relay process.
func (s *Store) ClaimDispatches(ctx context.Context, owner string, leaseDuration time.Duration, limit int) ([]queue.Dispatch, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("dispatcher owner is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("dispatch lease duration must be positive")
	}
	if limit <= 0 {
		limit = defaultDispatchBatch
	}
	rows, err := s.pool.Query(ctx, `WITH candidates AS (
 SELECT o.outbox_id FROM platform.dispatch_outbox o JOIN platform.execution e USING (tenant_id,app_id,request_id)
 WHERE o.status='PENDING' AND o.next_attempt_at <= clock_timestamp() AND e.status='PENDING'
 ORDER BY o.outbox_id FOR UPDATE SKIP LOCKED LIMIT $3
) UPDATE platform.dispatch_outbox o SET status='PUBLISHING', lease_owner=$1, lease_until=clock_timestamp()+$2::interval, attempt=attempt+1, updated_at=clock_timestamp()
FROM candidates c WHERE o.outbox_id=c.outbox_id RETURNING o.outbox_id,o.tenant_id,o.app_id,o.request_id`, owner, intervalLiteral(leaseDuration), limit)
	if err != nil {
		return nil, fmt.Errorf("claim dispatch outbox: %w", err)
	}
	defer rows.Close()
	var result []queue.Dispatch
	for rows.Next() {
		var d queue.Dispatch
		if err := rows.Scan(&d.OutboxID, &d.TenantID, &d.AppID, &d.RequestID); err != nil {
			return nil, fmt.Errorf("scan dispatch outbox: %w", err)
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dispatch outbox: %w", err)
	}
	return result, nil
}

// CompleteDispatch marks a successfully published stream message as sent.
func (s *Store) CompleteDispatch(ctx context.Context, dispatch queue.Dispatch, owner string) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='SENT', lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp() WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4 AND status='PUBLISHING' AND lease_owner=$5 AND lease_until > clock_timestamp()`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID, owner)
	if err != nil {
		return fmt.Errorf("complete dispatch: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.dispatch_outbox
WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("complete dispatch: %w", queue.ErrLeaseLost)
		}
		return fmt.Errorf("read dispatch completion: %w", err)
	}
	if status != "CONSUMED" {
		return fmt.Errorf("complete dispatch: %w", queue.ErrLeaseLost)
	}
	return nil
}

// RetryDispatch releases a relay claim so another publisher can retry it.
func (s *Store) RetryDispatch(ctx context.Context, dispatch queue.Dispatch, owner string, cause error) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='PENDING', lease_owner=NULL, lease_until=NULL, last_error=$6, next_attempt_at=clock_timestamp()+interval '1 second', updated_at=clock_timestamp() WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4 AND status='PUBLISHING' AND lease_owner=$5`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID, owner, truncateExecutionError(cause))
	if err != nil {
		return fmt.Errorf("retry dispatch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retry dispatch: %w", queue.ErrLeaseLost)
	}
	return nil
}

// RecoverDispatches makes expired execution and relay leases dispatchable again.
func (s *Store) RecoverDispatches(ctx context.Context, staleAfter time.Duration) error {
	if err := s.validate(); err != nil {
		return err
	}
	if staleAfter <= 0 {
		return errors.New("dispatch stale duration must be positive")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin dispatch recovery: %w", err)
	}
	defer func() { rollback(tx) }()
	if _, err = tx.Exec(ctx, `UPDATE platform.execution SET status='PENDING', lease_owner=NULL, run_token=NULL, lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp() WHERE status='RUNNING' AND lease_until <= clock_timestamp()`); err != nil {
		return fmt.Errorf("recover expired executions: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='PENDING', lease_owner=NULL, lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp() WHERE (status='PUBLISHING' AND lease_until <= clock_timestamp()) OR (status='SENT' AND updated_at <= clock_timestamp()-$1::interval)`, intervalLiteral(staleAfter)); err != nil {
		return fmt.Errorf("recover dispatch outbox: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM platform.dispatch_outbox WHERE status='CONSUMED' AND updated_at <= clock_timestamp() - $1::interval`, intervalLiteral(consumedOutboxRetention)); err != nil {
		return fmt.Errorf("clean consumed dispatch outbox: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id,app_id,request_id)
SELECT e.tenant_id,e.app_id,e.request_id
FROM platform.execution e
JOIN platform.tenant t ON t.tenant_id=e.tenant_id
JOIN platform.agent_app a ON a.tenant_id=e.tenant_id AND a.app_id=e.app_id
WHERE e.status='PENDING' AND t.status='ACTIVE' AND a.status='ACTIVE'
  AND NOT EXISTS (SELECT 1 FROM platform.dispatch_outbox o WHERE o.tenant_id=e.tenant_id AND o.app_id=e.app_id AND o.request_id=e.request_id AND o.status IN ('PENDING','PUBLISHING','SENT'))`); err != nil {
		return fmt.Errorf("fill missing dispatch outbox: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dispatch recovery: %w", err)
	}
	return nil
}

type storedExecution struct {
	tenantID, appID, requestID, sessionPrincipalID, sessionID, userID string
	turnSeq                                                           int64
	configVersion                                                     string
	tenantSource                                                      gateway.TenantSource
	command                                                           []byte
	traceID, status                                                   string
	attempt                                                           int
	nextAttemptAt, leaseUntil                                         time.Time
	hasEarlier                                                        bool
}

func lockExecutionForClaim(ctx context.Context, tx pgx.Tx, d queue.Dispatch) (storedExecution, bool, error) {
	var v storedExecution
	err := tx.QueryRow(ctx, `SELECT e.tenant_id,e.app_id,e.request_id,e.session_principal_id,e.session_id,e.user_id,e.turn_seq,e.config_version,e.tenant_source,e.command,e.trace_id,e.status,e.attempt,e.next_attempt_at,COALESCE(e.lease_until,'epoch'::timestamptz),EXISTS(SELECT 1 FROM platform.execution x WHERE x.tenant_id=e.tenant_id AND x.app_id=e.app_id AND x.session_principal_id=e.session_principal_id AND x.session_id=e.session_id AND x.turn_seq<e.turn_seq AND x.status IN ('PENDING','RUNNING')) FROM platform.execution e JOIN platform.tenant t ON t.tenant_id=e.tenant_id JOIN platform.agent_app a ON a.tenant_id=e.tenant_id AND a.app_id=e.app_id WHERE e.tenant_id=$1 AND e.app_id=$2 AND e.request_id=$3 FOR UPDATE OF e`, d.TenantID, d.AppID, d.RequestID).Scan(&v.tenantID, &v.appID, &v.requestID, &v.sessionPrincipalID, &v.sessionID, &v.userID, &v.turnSeq, &v.configVersion, &v.tenantSource, &v.command, &v.traceID, &v.status, &v.attempt, &v.nextAttemptAt, &v.leaseUntil, &v.hasEarlier)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, fmt.Errorf("lock dispatched execution: %w", err)
	}
	var active bool
	err = tx.QueryRow(ctx, `SELECT t.status='ACTIVE' AND a.status='ACTIVE' FROM platform.tenant t JOIN platform.agent_app a ON a.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND a.app_id=$2`, d.TenantID, d.AppID).Scan(&active)
	if err != nil {
		return v, false, fmt.Errorf("check execution availability: %w", err)
	}
	return v, active, nil
}
func (v storedExecution) executionJob() (execution.Job, error) {
	var c admissionCommand
	if err := json.Unmarshal(v.command, &c); err != nil {
		return execution.Job{}, fmt.Errorf("unmarshal execution command: %w", err)
	}
	if c.TenantID != v.tenantID || c.AppID != v.appID || c.SessionPrincipalID != v.sessionPrincipalID || c.SessionID != v.sessionID || c.UserID != v.userID {
		return execution.Job{}, errors.New("execution command does not match stored scope")
	}
	return execution.NewJob(v.requestID, v.tenantSource, tenant.RuntimeContext{TenantID: v.tenantID, AppID: v.appID, ConfigVersion: v.configVersion, SessionPrincipalID: v.sessionPrincipalID, SessionID: v.sessionID, UserID: v.userID, TraceID: v.traceID}, gateway.Message{Text: c.Text, ArtifactRefs: c.ArtifactRefs})
}
func consumeDispatch(ctx context.Context, tx pgx.Tx, id int64) error {
	_, err := tx.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='CONSUMED',lease_owner=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE outbox_id=$1`, id)
	if err != nil {
		return fmt.Errorf("consume dispatch: %w", err)
	}
	return nil
}
func deferDispatch(ctx context.Context, tx pgx.Tx, d queue.Dispatch, next time.Time) error {
	if next.Before(time.Now()) {
		next = time.Now().Add(blockedDispatchDelay)
	}
	if err := consumeDispatch(ctx, tx, d.OutboxID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id,app_id,request_id,next_attempt_at) VALUES ($1,$2,$3,$4)`, d.TenantID, d.AppID, d.RequestID, next)
	if err != nil {
		return fmt.Errorf("defer dispatch: %w", err)
	}
	return nil
}
func intervalLiteral(v time.Duration) string { return fmt.Sprintf("%d microseconds", v.Microseconds()) }
func truncateExecutionError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 512 {
		return value[:512]
	}
	return value
}
