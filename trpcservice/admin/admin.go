// Package admin defines the control-plane boundary for tenant configuration.
package admin

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	memorytencentdb "github.com/liuzengh/trpc-agent-service/trpcservice/memory/tencentdb"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	apiKeyEntropyBytes       = 32
	credentialIDEntropyBytes = 16
	apiKeyPrefixLength       = 12
)

// Repository provides the durable control-plane operations used by API.
// Implementations must preserve config immutability and credential revocation.
type Repository interface {
	CreateTenant(ctx context.Context, value tenant.Tenant) error
	CreateAgentApp(ctx context.Context, app tenant.AgentApp, initial tenant.AppConfig) error
	CreateChannelBinding(ctx context.Context, binding channels.Binding) error
	InsertAppConfigVersion(ctx context.Context, cfg tenant.AppConfig) error
	ActivateAppConfig(ctx context.Context, tenantID, appID, version string) error
	CreateCredential(ctx context.Context, digest auth.APIKeyDigest, credential auth.Credential) error
	RevokeCredential(ctx context.Context, tenantID, appID, credentialID string) error
}

type auditReader interface {
	ListAuditEvents(context.Context, string, string, int) ([]platformaudit.Event, error)
}

type auditQueryReader interface {
	ListAuditEventsQuery(context.Context, platformaudit.Query) ([]platformaudit.Event, error)
}

type tenantReader interface {
	ResolveTenant(context.Context, string) (tenant.Tenant, error)
}

type approvalRepository interface {
	platformapproval.Repository
}

type dataMigrationRepository interface {
	CreateDataMigration(context.Context, migration.Record) error
	BeginDataMigration(context.Context, string, string, string, string, time.Time, time.Duration) (migration.Record, error)
}

type inputError struct {
	cause error
}

func (e *inputError) Error() string {
	return e.cause.Error()
}

func (e *inputError) Unwrap() error {
	return e.cause
}

func invalidInput(err error) error {
	if err == nil {
		return nil
	}
	return &inputError{cause: err}
}

// ProvisionChannelBinding is the canonical channel binding creation path. It
// generates a public route only for the remaining HTTP channel and sets the
// initial revision before persisting the tenant-owned binding. Callers must
// not provide either generated field.
func (a API) ProvisionChannelBinding(
	ctx context.Context,
	binding channels.Binding,
) (channels.Binding, error) {
	prepared, err := prepareChannelBinding(binding)
	if err != nil {
		return channels.Binding{}, err
	}
	repository, err := a.repository()
	if err != nil {
		return channels.Binding{}, err
	}
	if err := repository.CreateChannelBinding(ctx, prepared); err != nil {
		return channels.Binding{}, fmt.Errorf("create channel binding: %w", err)
	}
	return prepared, nil
}

func prepareChannelBinding(binding channels.Binding) (channels.Binding, error) {
	if binding.PublicRouteID != "" {
		return channels.Binding{}, &inputError{
			cause: errors.New("public_route_id must be omitted when creating a channel binding"),
		}
	}
	if binding.BindingRevision != 0 {
		return channels.Binding{}, &inputError{
			cause: errors.New("binding_revision must be omitted when creating a channel binding"),
		}
	}
	if binding.Channel == channels.ChannelWeChatCustomer {
		publicRouteID, err := channels.NewPublicRouteID()
		if err != nil {
			return channels.Binding{}, err
		}
		binding.PublicRouteID = publicRouteID
	}
	binding.BindingRevision = 1
	if err := binding.Validate(); err != nil {
		return channels.Binding{}, &inputError{cause: err}
	}
	return binding, nil
}

// API validates tenant application configuration before publishing it.
type API struct {
	Bindings   config.BindingResolver
	Repository Repository
}

// ValidateAppConfig checks app config fields and cross-resource references.
func (a API) ValidateAppConfig(ctx context.Context, cfg tenant.AppConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if reader, ok := a.Repository.(tenantReader); ok {
		value, err := reader.ResolveTenant(ctx, cfg.TenantID)
		if err != nil {
			return fmt.Errorf("resolve tenant audit policy: %w", err)
		}
		if err := value.Audit.ValidateAppConfig(cfg.Audit); err != nil {
			return err
		}
	}
	if err := platformsession.ValidateBackend(cfg.BackendConfig.Session); err != nil {
		return fmt.Errorf("session backend provider: %w", err)
	}
	if !cfg.BackendConfig.Memory.IsZero() {
		if err := memorytencentdb.ValidateBackend(cfg.BackendConfig.Memory); err != nil {
			return fmt.Errorf("memory backend: %w", err)
		}
	}
	if !cfg.BackendConfig.Artifact.IsZero() {
		if err := artifactcos.ValidateBackend(cfg.BackendConfig.Artifact); err != nil {
			return fmt.Errorf("artifact backend: %w", err)
		}
	}
	if !cfg.BackendConfig.Knowledge.IsZero() {
		if err := knowledgeqdrant.ValidateBackend(cfg.BackendConfig.Knowledge); err != nil {
			return fmt.Errorf("knowledge backend: %w", err)
		}
	}
	if err := config.ValidateAppConfigBindings(ctx, cfg, a.Bindings); err != nil {
		return fmt.Errorf("channel bindings: %w", err)
	}
	return nil
}

// CreateTenant validates and persists a new tenant control-plane record.
func (a API) CreateTenant(ctx context.Context, value tenant.Tenant) error {
	if err := value.Validate(); err != nil {
		return invalidInput(err)
	}
	repository, err := a.repository()
	if err != nil {
		return err
	}
	if err := repository.CreateTenant(ctx, value); err != nil {
		return fmt.Errorf("create tenant: %w", err)
	}
	return nil
}

// CreateAgentApp validates and persists an application with its first immutable
// configuration version.
func (a API) CreateAgentApp(
	ctx context.Context,
	app tenant.AgentApp,
	initial tenant.AppConfig,
) error {
	if err := app.Validate(); err != nil {
		return invalidInput(err)
	}
	if err := a.ValidateAppConfig(ctx, initial); err != nil {
		return invalidInput(fmt.Errorf("initial app config: %w", err))
	}
	if len(initial.KnowledgeBaseIDs) != 0 {
		return invalidInput(errors.New("initial app config cannot bind knowledge bases before the app exists"))
	}
	if app.TenantID != initial.TenantID || app.AppID != initial.AppID {
		return invalidInput(errors.New("initial app config does not match agent app scope"))
	}
	if app.ActiveConfigVersion != initial.Version {
		return invalidInput(errors.New("active_config_version does not match initial app config"))
	}
	repository, err := a.repository()
	if err != nil {
		return err
	}
	if err := repository.CreateAgentApp(ctx, app, initial); err != nil {
		return fmt.Errorf("create agent app: %w", err)
	}
	return nil
}

// PublishAppConfig validates and persists a new immutable configuration
// version. It does not change the active version.
func (a API) PublishAppConfig(ctx context.Context, cfg tenant.AppConfig) error {
	if err := a.ValidateAppConfig(ctx, cfg); err != nil {
		return invalidInput(err)
	}
	repository, err := a.repository()
	if err != nil {
		return err
	}
	if err := repository.InsertAppConfigVersion(ctx, cfg); err != nil {
		return fmt.Errorf("publish app config: %w", err)
	}
	return nil
}

// ActivateAppConfig changes the version used by new admissions in one tenant
// application scope. Existing jobs retain their admitted config version.
func (a API) ActivateAppConfig(ctx context.Context, scope tenant.Scope, version string) error {
	if err := scope.Validate(); err != nil {
		return invalidInput(err)
	}
	if version == "" {
		return invalidInput(errors.New("config version is required"))
	}
	repository, err := a.repository()
	if err != nil {
		return err
	}
	if err := repository.ActivateAppConfig(ctx, scope.TenantID, scope.AppID, version); err != nil {
		return fmt.Errorf("activate app config: %w", err)
	}
	return nil
}

// CreateDataMigration records a PENDING backend migration between immutable
// config versions. It does not block admissions until BeginDataMigration.
func (a API) CreateDataMigration(
	ctx context.Context,
	scope tenant.Scope,
	sourceVersion, targetVersion string,
) (migration.Record, error) {
	if err := scope.Validate(); err != nil {
		return migration.Record{}, invalidInput(err)
	}
	repository, err := a.dataMigrationRepository()
	if err != nil {
		return migration.Record{}, err
	}
	record := migration.Record{
		ID:                  uuid.NewString(),
		TenantID:            scope.TenantID,
		AppID:               scope.AppID,
		SourceConfigVersion: sourceVersion,
		TargetConfigVersion: targetVersion,
		Status:              migration.StatusPending,
	}
	if err := record.Validate(); err != nil {
		return migration.Record{}, invalidInput(err)
	}
	if err := repository.CreateDataMigration(ctx, record); err != nil {
		return migration.Record{}, fmt.Errorf("create data migration: %w", err)
	}
	return record, nil
}

// BeginDataMigration enters DRAINING and starts the authoritative admission
// gate for a previously recorded data migration.
func (a API) BeginDataMigration(
	ctx context.Context,
	scope tenant.Scope,
	migrationID, owner string,
	drainDeadline time.Time,
	leaseDuration time.Duration,
) (migration.Record, error) {
	if err := scope.Validate(); err != nil {
		return migration.Record{}, invalidInput(err)
	}
	if migrationID == "" || owner == "" || leaseDuration <= 0 {
		return migration.Record{}, invalidInput(errors.New("data migration id, owner, and lease duration are required"))
	}
	if !drainDeadline.After(time.Now()) {
		return migration.Record{}, invalidInput(errors.New("data migration drain deadline must be in the future"))
	}
	repository, err := a.dataMigrationRepository()
	if err != nil {
		return migration.Record{}, err
	}
	record, err := repository.BeginDataMigration(
		ctx, scope.TenantID, scope.AppID, migrationID, owner, drainDeadline, leaseDuration,
	)
	if err != nil {
		return migration.Record{}, fmt.Errorf("begin data migration: %w", err)
	}
	return record, nil
}

// IssuedCredential contains the one-time raw API key and its safe metadata.
// Callers must deliver APIKey only to the administrator who requested it.
type IssuedCredential struct {
	Credential auth.Credential
	APIKey     string
}

// IssueCredential generates a high-entropy API key for one tenant application
// scope. Only its SHA-256 digest is persisted; APIKey is returned once.
func (a API) IssueCredential(
	ctx context.Context,
	scope tenant.Scope,
	expiresAt time.Time,
) (IssuedCredential, error) {
	if err := scope.Validate(); err != nil {
		return IssuedCredential{}, invalidInput(err)
	}
	if !expiresAt.IsZero() && !expiresAt.After(time.Now()) {
		return IssuedCredential{}, invalidInput(errors.New("credential expiry must be in the future"))
	}
	apiKey, credentialID, err := generateCredentialValues(rand.Reader)
	if err != nil {
		return IssuedCredential{}, err
	}
	digest, err := auth.DigestAPIKey(apiKey)
	if err != nil {
		return IssuedCredential{}, err
	}
	credential := auth.Credential{
		ID:        credentialID,
		TenantID:  scope.TenantID,
		AppID:     scope.AppID,
		KeyPrefix: apiKey[:apiKeyPrefixLength],
		Status:    auth.CredentialActive,
		ExpiresAt: expiresAt,
	}
	if err := credential.Validate(); err != nil {
		return IssuedCredential{}, invalidInput(err)
	}
	repository, err := a.repository()
	if err != nil {
		return IssuedCredential{}, err
	}
	if err := repository.CreateCredential(ctx, digest, credential); err != nil {
		return IssuedCredential{}, fmt.Errorf("create api credential: %w", err)
	}
	return IssuedCredential{Credential: credential, APIKey: apiKey}, nil
}

// RevokeCredential permanently deactivates one API credential in tenant
// application scope.
func (a API) RevokeCredential(ctx context.Context, scope tenant.Scope, credentialID string) error {
	if err := scope.Validate(); err != nil {
		return invalidInput(err)
	}
	if credentialID == "" {
		return invalidInput(errors.New("credential_id is required"))
	}
	repository, err := a.repository()
	if err != nil {
		return err
	}
	if err := repository.RevokeCredential(ctx, scope.TenantID, scope.AppID, credentialID); err != nil {
		return fmt.Errorf("revoke api credential: %w", err)
	}
	return nil
}

// ListAuditEvents returns metadata-only audit events from exactly one tenant
// application scope. The concrete repository owns the SQL scope predicate.
func (a API) ListAuditEvents(ctx context.Context, scope tenant.Scope, limit int) ([]platformaudit.Event, error) {
	if err := scope.Validate(); err != nil {
		return nil, invalidInput(err)
	}
	query := platformaudit.Query{
		TenantID: scope.TenantID,
		AppID:    scope.AppID,
		Limit:    limit,
	}
	if err := query.Validate(); err != nil {
		return nil, invalidInput(err)
	}
	reader, err := a.auditReader()
	if err != nil {
		return nil, err
	}
	var events []platformaudit.Event
	if queryReader, ok := a.Repository.(auditQueryReader); ok {
		events, err = queryReader.ListAuditEventsQuery(ctx, query)
	} else {
		events, err = reader.ListAuditEvents(ctx, query.TenantID, query.AppID, query.Limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	return events, nil
}

// ListAuditEventsQuery returns metadata-only audit events with optional
// operator filters while preserving exact tenant/application scope.
func (a API) ListAuditEventsQuery(ctx context.Context, query platformaudit.Query) ([]platformaudit.Event, error) {
	if err := query.Validate(); err != nil {
		return nil, invalidInput(err)
	}
	reader, ok := a.Repository.(auditQueryReader)
	if !ok {
		return nil, errors.New("admin repository does not support audit queries")
	}
	events, err := reader.ListAuditEventsQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list audit events: %w", err)
	}
	return events, nil
}

// ListApprovals returns metadata-only approval rows from one exact scope.
func (a API) ListApprovals(ctx context.Context, query platformapproval.Query) ([]platformapproval.Record, error) {
	if err := query.Validate(); err != nil {
		return nil, invalidInput(err)
	}
	repository, err := a.approvalRepository()
	if err != nil {
		return nil, err
	}
	approvals, err := repository.List(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list approvals: %w", err)
	}
	return approvals, nil
}

// DecideApproval records one administrator decision in exact tenant/app
// scope. The repository makes repeated identical decisions idempotent.
func (a API) DecideApproval(ctx context.Context, scope tenant.Scope, approvalID string, status platformapproval.Status) (platformapproval.Record, error) {
	if err := scope.Validate(); err != nil {
		return platformapproval.Record{}, invalidInput(err)
	}
	if approvalID == "" {
		return platformapproval.Record{}, invalidInput(errors.New("approval_id is required"))
	}
	if status != platformapproval.StatusApproved && status != platformapproval.StatusDenied {
		return platformapproval.Record{}, invalidInput(errors.New("approval decision must be approved or denied"))
	}
	repository, err := a.approvalRepository()
	if err != nil {
		return platformapproval.Record{}, err
	}
	record, err := repository.Decide(ctx, scope.TenantID, scope.AppID, approvalID, status)
	if err != nil {
		return platformapproval.Record{}, fmt.Errorf("decide approval: %w", err)
	}
	return record, nil
}

func (a API) repository() (Repository, error) {
	if a.Repository == nil {
		return nil, errors.New("admin repository is required")
	}
	return a.Repository, nil
}

func (a API) dataMigrationRepository() (dataMigrationRepository, error) {
	repository, err := a.repository()
	if err != nil {
		return nil, err
	}
	migrations, ok := repository.(dataMigrationRepository)
	if !ok {
		return nil, errors.New("admin repository does not support data migrations")
	}
	return migrations, nil
}

func (a API) auditReader() (auditReader, error) {
	repository, err := a.repository()
	if err != nil {
		return nil, err
	}
	reader, ok := repository.(auditReader)
	if !ok {
		return nil, errors.New("admin repository does not support audit queries")
	}
	return reader, nil
}

func (a API) approvalRepository() (approvalRepository, error) {
	repository, err := a.repository()
	if err != nil {
		return nil, err
	}
	approvals, ok := repository.(approvalRepository)
	if !ok {
		return nil, errors.New("admin repository does not support approvals")
	}
	return approvals, nil
}

func generateCredentialValues(random io.Reader) (string, string, error) {
	if random == nil {
		return "", "", errors.New("credential random source is required")
	}
	apiKeyBytes := make([]byte, apiKeyEntropyBytes)
	if _, err := io.ReadFull(random, apiKeyBytes); err != nil {
		return "", "", fmt.Errorf("generate api key: %w", err)
	}
	credentialIDBytes := make([]byte, credentialIDEntropyBytes)
	if _, err := io.ReadFull(random, credentialIDBytes); err != nil {
		return "", "", fmt.Errorf("generate credential id: %w", err)
	}
	apiKey := "tas_" + base64.RawURLEncoding.EncodeToString(apiKeyBytes)
	credentialID := "cred_" + base64.RawURLEncoding.EncodeToString(credentialIDBytes)
	return apiKey, credentialID, nil
}
