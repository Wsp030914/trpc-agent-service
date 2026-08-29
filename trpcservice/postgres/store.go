package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const rollbackTimeout = 5 * time.Second

var (
	// ErrNotFound means the requested platform record does not exist.
	ErrNotFound = errors.New("postgres record not found")
)

var (
	_ auth.CredentialStore = (*Store)(nil)
	_ auth.Directory       = (*Store)(nil)
	_ config.Resolver      = (*Store)(nil)
	_ gateway.Admitter     = (*Store)(nil)
)

// Store is the concrete PostgreSQL implementation for platform-owned records.
// Callers should depend on capability interfaces from the consuming package,
// such as auth.CredentialStore, auth.Directory, or config.Resolver, rather
// than on a cross-backend Store interface. The caller owns the supplied pool
// and must close it after all Store operations have stopped. Store is safe for
// concurrent use.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a Store using a caller-owned PostgreSQL pool.
func New(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("postgres pool is required")
	}
	return &Store{pool: pool}, nil
}

// CreateTenant inserts a tenant control-plane record.
func (s *Store) CreateTenant(ctx context.Context, value tenant.Tenant) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("tenant: %w", err)
	}
	auditPolicy, err := marshalAuditPolicy(value.Audit)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(
		ctx,
		`INSERT INTO platform.tenant (tenant_id, name, status, audit_policy)
VALUES ($1, $2, $3, $4)`,
		value.ID,
		value.Name,
		value.Status,
		auditPolicy,
	); err != nil {
		return fmt.Errorf("create tenant: %w", err)
	}
	return nil
}

// ResolveTenant returns one tenant by its exact identifier.
func (s *Store) ResolveTenant(ctx context.Context, tenantID string) (tenant.Tenant, error) {
	if err := s.validate(); err != nil {
		return tenant.Tenant{}, err
	}
	if tenantID == "" {
		return tenant.Tenant{}, errors.New("tenant_id is required")
	}
	var value tenant.Tenant
	var auditPolicy []byte
	err := s.pool.QueryRow(
		ctx,
		`SELECT tenant_id, name, status, audit_policy
FROM platform.tenant
WHERE tenant_id = $1`,
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

// CreateAgentApp inserts an application and its initial immutable config in
// one transaction. The active config version must equal initial.Version.
func (s *Store) CreateAgentApp(
	ctx context.Context,
	app tenant.AgentApp,
	initial tenant.AppConfig,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := app.Validate(); err != nil {
		return fmt.Errorf("agent app: %w", err)
	}
	if err := initial.Validate(); err != nil {
		return fmt.Errorf("initial app config: %w", err)
	}
	if err := config.ValidateAppConfigBindings(ctx, initial, s); err != nil {
		return fmt.Errorf("initial app config channel bindings: %w", err)
	}
	if len(initial.KnowledgeBaseIDs) != 0 {
		return errors.New("initial app config cannot bind knowledge bases before the app exists")
	}
	if app.TenantID != initial.TenantID || app.AppID != initial.AppID {
		return errors.New("initial app config does not match agent app scope")
	}
	if app.ActiveConfigVersion != initial.Version {
		return errors.New("active_config_version does not match initial app config")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create agent app: %w", err)
	}
	defer func() {
		rollback(tx)
	}()
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.agent_app
    (tenant_id, app_id, name, active_config_version, status)
VALUES ($1, $2, $3, $4, $5)`,
		app.TenantID,
		app.AppID,
		app.Name,
		app.ActiveConfigVersion,
		app.Status,
	); err != nil {
		return fmt.Errorf("create agent app: %w", err)
	}
	if err := insertAppConfig(ctx, tx, initial); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create agent app: %w", err)
	}
	return nil
}

// ResolveAgentApp returns one application in exact tenant scope.
func (s *Store) ResolveAgentApp(
	ctx context.Context,
	tenantID string,
	appID string,
) (tenant.AgentApp, error) {
	if err := s.validate(); err != nil {
		return tenant.AgentApp{}, err
	}
	if tenantID == "" {
		return tenant.AgentApp{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return tenant.AgentApp{}, errors.New("app_id is required")
	}
	var app tenant.AgentApp
	err := s.pool.QueryRow(
		ctx,
		`SELECT tenant_id, app_id, name, active_config_version, status
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2`,
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

// InsertAppConfigVersion inserts a new immutable application config version.
// It does not change the application's active version.
func (s *Store) InsertAppConfigVersion(ctx context.Context, cfg tenant.AppConfig) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := config.ValidateAppConfigBindings(ctx, cfg, s); err != nil {
		return fmt.Errorf("app config channel bindings: %w", err)
	}
	if err := s.validateKnowledgeBaseIDs(ctx, cfg); err != nil {
		return err
	}
	return insertAppConfig(ctx, s.pool, cfg)
}

// ActivateAppConfig changes which immutable config version new admissions use.
// Session, Memory, and Artifact backend changes require a data migration.
// Knowledge is derived: a changed generation must be fully indexed before the
// active version can switch.
func (s *Store) ActivateAppConfig(ctx context.Context, tenantID, appID, version string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if appID == "" {
		return errors.New("app_id is required")
	}
	if version == "" {
		return errors.New("config version is required")
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin activate app config: %w", err)
	}
	defer func() {
		rollback(tx)
	}()
	var activeVersion string
	if err := tx.QueryRow(
		ctx,
		`SELECT active_config_version
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE`,
		tenantID,
		appID,
	).Scan(&activeVersion); err != nil {
		return fmt.Errorf("lock agent app: %w", resolveError("agent app", err))
	}
	if activeVersion == version {
		return tx.Commit(ctx)
	}
	blocked, err := activeDataMigrationExists(ctx, tx, tenantID, appID)
	if err != nil {
		return err
	}
	if blocked {
		return errors.New("app config activation is blocked by data migration")
	}
	activeBackend, err := storedBackendConfig(ctx, tx, tenantID, appID, activeVersion)
	if err != nil {
		return err
	}
	targetBackend, err := storedBackendConfig(ctx, tx, tenantID, appID, version)
	if err != nil {
		return err
	}
	if !sameAuthoritativeBackends(activeBackend, targetBackend) {
		return errors.New("config activation changes authoritative backends; migrate the backends before switching the active version")
	}
	if !sameBackendRef(activeBackend.Knowledge, targetBackend.Knowledge) && !targetBackend.Knowledge.IsZero() {
		generation := targetBackend.Knowledge.Options["index_generation"]
		if generation == "" {
			return errors.New("knowledge backend index_generation is required")
		}
		if !activeBackend.Knowledge.IsZero() && activeBackend.Knowledge.Options["index_generation"] == generation {
			return errors.New("knowledge backend changes require a new index_generation")
		}
		scope := tenant.Scope{TenantID: tenantID, AppID: appID}
		target, err := resolveAppConfigFrom(ctx, tx, tenantID, appID, version)
		if err != nil {
			return err
		}
		buildID, buildStatus, err := ensureKnowledgeGenerationBuild(ctx, tx, scope, version, generation)
		if err != nil {
			return err
		}
		if buildStatus == platformknowledge.IndexJobFailed {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit failed knowledge generation: %w", err)
			}
			return fmt.Errorf("%w for config %q", tenant.ErrKnowledgeGenerationFailed, version)
		}
		if _, err := enqueueKnowledgeGeneration(ctx, tx, scope, version, buildID, target.KnowledgeBaseIDs, generation); err != nil {
			return err
		}
		cause, failed, err := failedKnowledgeGenerationJob(ctx, tx, buildID)
		if err != nil {
			return err
		}
		if failed {
			if err := failKnowledgeGenerationBuild(ctx, tx, buildID, cause); err != nil {
				return err
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit failed knowledge generation: %w", err)
			}
			return fmt.Errorf("%w for config %q", tenant.ErrKnowledgeGenerationFailed, version)
		}
		ready, err := knowledgeGenerationReady(ctx, tx, scope, version, buildID, target.KnowledgeBaseIDs, generation)
		if err != nil {
			return err
		}
		if !ready {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("commit knowledge generation rebuild: %w", err)
			}
			return fmt.Errorf("%w for config %q", tenant.ErrKnowledgeGenerationPending, version)
		}
		if err := completeKnowledgeGenerationBuild(ctx, tx, buildID); err != nil {
			return err
		}
	}
	commandTag, err := tx.Exec(
		ctx,
		`UPDATE platform.agent_app
SET active_config_version = $3, updated_at = now()
WHERE tenant_id = $1 AND app_id = $2`,
		tenantID,
		appID,
		version,
	)
	if err != nil {
		return fmt.Errorf("activate app config: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("agent app config: %w", ErrNotFound)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit activate app config: %w", err)
	}
	return nil
}

// RebuildKnowledgeGeneration creates a fresh immutable build after a terminal
// indexing failure. The target config remains inactive until ActivateAppConfig
// observes that every source has completed the new build.
func (s *Store) RebuildKnowledgeGeneration(ctx context.Context, tenantID, appID, version string) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" || appID == "" || version == "" {
		return errors.New("tenant_id, app_id, and config version are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rebuild knowledge generation: %w", err)
	}
	defer func() { rollback(tx) }()
	var activeVersion string
	if err := tx.QueryRow(ctx, `
SELECT active_config_version
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE`, tenantID, appID).Scan(&activeVersion); err != nil {
		return fmt.Errorf("lock agent app: %w", resolveError("agent app", err))
	}
	if activeVersion == version {
		return errors.New("active config does not require a knowledge rebuild")
	}
	target, err := resolveAppConfigFrom(ctx, tx, tenantID, appID, version)
	if err != nil {
		return err
	}
	if target.BackendConfig.Knowledge.IsZero() {
		return errors.New("knowledge backend is not configured")
	}
	generation := target.BackendConfig.Knowledge.Options["index_generation"]
	if generation == "" {
		return errors.New("knowledge backend index_generation is required")
	}
	scope := tenant.Scope{TenantID: tenantID, AppID: appID}
	_, status, err := ensureKnowledgeGenerationBuild(ctx, tx, scope, version, generation)
	if err != nil {
		return err
	}
	if status != platformknowledge.IndexJobFailed {
		return errors.New("knowledge generation rebuild is not failed")
	}
	buildID := uuid.NewString()
	if _, err := tx.Exec(ctx, `
UPDATE platform.knowledge_generation_build
SET build_id = $2, status = 'PENDING', last_error = '', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $3 AND config_version = $4 AND status = 'FAILED'`,
		tenantID, buildID, appID, version); err != nil {
		return fmt.Errorf("restart knowledge generation build: %w", err)
	}
	if _, err := enqueueKnowledgeGeneration(ctx, tx, scope, version, buildID, target.KnowledgeBaseIDs, generation); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rebuild knowledge generation: %w", err)
	}
	return nil
}

func storedBackendConfig(
	ctx context.Context,
	db databaseQueryer,
	tenantID, appID, version string,
) (tenant.BackendConfig, error) {
	var encoded []byte
	err := db.QueryRow(
		ctx,
		`SELECT backend_config
FROM platform.app_config_version
WHERE tenant_id = $1 AND app_id = $2 AND version = $3 AND status = 'PUBLISHED'`,
		tenantID,
		appID,
		version,
	).Scan(&encoded)
	if err != nil {
		return tenant.BackendConfig{}, fmt.Errorf("resolve app config %s: %w", version, resolveError("app config", err))
	}
	var document backendConfigDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return tenant.BackendConfig{}, fmt.Errorf("unmarshal app config %s: %w", version, err)
	}
	return document.value(), nil
}

func sameAuthoritativeBackends(a, b tenant.BackendConfig) bool {
	return sameBackendRef(a.Session, b.Session) &&
		sameBackendRef(a.Memory, b.Memory) &&
		sameBackendRef(a.Artifact, b.Artifact)
}

func sameBackendRef(a, b tenant.BackendRef) bool {
	return a.Kind == b.Kind &&
		a.Provider == b.Provider &&
		a.Name == b.Name &&
		a.SecretRef == b.SecretRef &&
		a.DSNRef == b.DSNRef &&
		reflect.DeepEqual(a.Options, b.Options)
}

// ResolveAppConfig returns one exact immutable application config version.
func (s *Store) ResolveAppConfig(
	ctx context.Context,
	tenantID string,
	appID string,
	version string,
) (tenant.AppConfig, error) {
	if err := s.validate(); err != nil {
		return tenant.AppConfig{}, err
	}
	if tenantID == "" {
		return tenant.AppConfig{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return tenant.AppConfig{}, errors.New("app_id is required")
	}
	if version == "" {
		return tenant.AppConfig{}, errors.New("config version is required")
	}

	var modelConfig []byte
	var toolPolicy []byte
	var backendConfig []byte
	var auditPolicy []byte
	var secretRefs []byte
	var channelBindingIDs []byte
	var knowledgeBaseIDs []byte
	err := s.pool.QueryRow(
		ctx,
		`SELECT
    model_config,
    tool_policy,
    backend_config,
    audit_policy,
    secret_refs,
    channel_binding_ids,
    knowledge_base_ids
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
		&knowledgeBaseIDs,
	)
	if err != nil {
		return tenant.AppConfig{}, resolveError("app config", err)
	}
	cfg, err := unmarshalAppConfig(
		tenantID,
		appID,
		version,
		modelConfig,
		toolPolicy,
		backendConfig,
		auditPolicy,
		secretRefs,
		channelBindingIDs,
		knowledgeBaseIDs,
	)
	if err != nil {
		return tenant.AppConfig{}, err
	}
	if err := config.ValidateAppConfigBindings(ctx, cfg, s); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("stored app config channel bindings: %w", err)
	}
	return cfg, nil
}

// CreateCredential inserts API credential metadata and its one-way key digest.
// The raw API key must not be passed to or persisted by Store.
func (s *Store) CreateCredential(
	ctx context.Context,
	digest auth.APIKeyDigest,
	credential auth.Credential,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := credential.Validate(); err != nil {
		return fmt.Errorf("api credential: %w", err)
	}
	if digest == (auth.APIKeyDigest{}) {
		return errors.New("api key digest is required")
	}
	var expiresAt any
	if !credential.ExpiresAt.IsZero() {
		expiresAt = credential.ExpiresAt
	}
	if _, err := s.pool.Exec(
		ctx,
		`INSERT INTO platform.api_credential
    (tenant_id, app_id, credential_id, key_digest, key_prefix, status, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		credential.TenantID,
		credential.AppID,
		credential.ID,
		digest[:],
		credential.KeyPrefix,
		credential.Status,
		expiresAt,
	); err != nil {
		return fmt.Errorf("create api credential: %w", err)
	}
	return nil
}

// RevokeCredential permanently deactivates one API credential in exact tenant
// application scope. The update locks the same row used by Admission, so an
// admission and revocation have one database commit order.
func (s *Store) RevokeCredential(
	ctx context.Context,
	tenantID string,
	appID string,
	credentialID string,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" {
		return errors.New("tenant_id is required")
	}
	if appID == "" {
		return errors.New("app_id is required")
	}
	if credentialID == "" {
		return errors.New("credential_id is required")
	}
	commandTag, err := s.pool.Exec(
		ctx,
		`UPDATE platform.api_credential
SET status = 'REVOKED'
WHERE tenant_id = $1 AND app_id = $2 AND credential_id = $3`,
		tenantID,
		appID,
		credentialID,
	)
	if err != nil {
		return fmt.Errorf("revoke api credential: %w", err)
	}
	if commandTag.RowsAffected() == 0 {
		return fmt.Errorf("api credential: %w", ErrNotFound)
	}
	return nil
}

// ResolveAPIKey returns API credential metadata for an exact key digest.
func (s *Store) ResolveAPIKey(
	ctx context.Context,
	digest auth.APIKeyDigest,
) (auth.Credential, error) {
	if err := s.validate(); err != nil {
		return auth.Credential{}, err
	}
	if digest == (auth.APIKeyDigest{}) {
		return auth.Credential{}, errors.New("api key digest is required")
	}
	var credential auth.Credential
	var expiresAt *time.Time
	err := s.pool.QueryRow(
		ctx,
		`SELECT credential_id, tenant_id, app_id, key_prefix, status, expires_at
FROM platform.api_credential
WHERE key_digest = $1`,
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
		if errors.Is(err, pgx.ErrNoRows) {
			return auth.Credential{}, fmt.Errorf("%w: %w", auth.ErrCredentialNotFound, ErrNotFound)
		}
		return auth.Credential{}, resolveError("api credential", err)
	}
	if expiresAt != nil {
		credential.ExpiresAt = expiresAt.UTC()
	}
	if err := credential.Validate(); err != nil {
		return auth.Credential{}, fmt.Errorf("stored api credential: %w", err)
	}
	return credential, nil
}

type databaseExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func insertAppConfig(ctx context.Context, db databaseExecutor, cfg tenant.AppConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("app config: %w", err)
	}
	modelConfig, toolPolicy, backendConfig, auditPolicy, secretRefs, bindings, knowledgeBaseIDs, err :=
		marshalAppConfig(cfg)
	if err != nil {
		return err
	}
	if _, err := db.Exec(
		ctx,
		`INSERT INTO platform.app_config_version (
    tenant_id,
    app_id,
    version,
    model_config,
    tool_policy,
    backend_config,
    audit_policy,
    secret_refs,
    channel_binding_ids,
    knowledge_base_ids
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		cfg.TenantID,
		cfg.AppID,
		cfg.Version,
		modelConfig,
		toolPolicy,
		backendConfig,
		auditPolicy,
		secretRefs,
		bindings,
		knowledgeBaseIDs,
	); err != nil {
		return fmt.Errorf("insert app config: %w", err)
	}
	return nil
}

func (s *Store) validate() error {
	if s == nil || s.pool == nil {
		return errors.New("postgres store is not initialized")
	}
	return nil
}

func (s *Store) validateKnowledgeBaseIDs(ctx context.Context, cfg tenant.AppConfig) error {
	if len(cfg.KnowledgeBaseIDs) == 0 {
		return nil
	}
	var count int
	if err := s.pool.QueryRow(ctx, `
SELECT count(*)
FROM platform.knowledge_base
WHERE tenant_id = $1
  AND app_id = $2
  AND status = 'ACTIVE'
  AND knowledge_base_id = ANY($3)`,
		cfg.TenantID,
		cfg.AppID,
		cfg.KnowledgeBaseIDs,
	).Scan(&count); err != nil {
		return fmt.Errorf("validate knowledge base bindings: %w", err)
	}
	if count != len(cfg.KnowledgeBaseIDs) {
		return errors.New("knowledge base binding is missing or inactive")
	}
	return nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func resolveError(entity string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", entity, ErrNotFound)
	}
	return fmt.Errorf("resolve %s: %w", entity, err)
}

type modelConfigDocument struct {
	Provider   string            `json:"provider"`
	Model      string            `json:"model"`
	APIKeyRef  secretRefDocument `json:"api_key_ref,omitempty"`
	Parameters map[string]string `json:"parameters,omitempty"`
}

type toolPolicyDocument struct {
	VisibleTools    []string `json:"visible_tools,omitempty"`
	ExecutableTools []string `json:"executable_tools,omitempty"`
}

type backendConfigDocument struct {
	Name      string             `json:"name"`
	Session   backendRefDocument `json:"session"`
	Memory    backendRefDocument `json:"memory,omitempty"`
	Knowledge backendRefDocument `json:"knowledge,omitempty"`
	Artifact  backendRefDocument `json:"artifact,omitempty"`
}

type backendRefDocument struct {
	Kind      tenant.BackendKind `json:"kind,omitempty"`
	Provider  string             `json:"provider,omitempty"`
	Name      string             `json:"name,omitempty"`
	SecretRef secretRefDocument  `json:"secret_ref,omitempty"`
	DSNRef    string             `json:"dsn_ref,omitempty"`
	Options   map[string]string  `json:"options,omitempty"`
}

type auditPolicyDocument struct {
	Enabled       bool `json:"enabled"`
	RetentionDays int  `json:"retention_days"`
	RedactPII     bool `json:"redact_pii"`
}

type secretRefDocument struct {
	Name    string `json:"name"`
	Version string `json:"version,omitempty"`
}

func marshalAppConfig(cfg tenant.AppConfig) (
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	[]byte,
	error,
) {
	modelConfig, err := json.Marshal(modelConfigDocument{
		Provider:   cfg.Model.Provider,
		Model:      cfg.Model.Model,
		APIKeyRef:  newSecretRefDocument(cfg.Model.APIKeyRef),
		Parameters: cfg.Model.Parameters,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal model config: %w", err)
	}
	toolPolicy, err := json.Marshal(toolPolicyDocument{
		VisibleTools:    cfg.Tools.VisibleTools,
		ExecutableTools: cfg.Tools.ExecutableTools,
	})
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal tool policy: %w", err)
	}
	backendConfig, err := json.Marshal(newBackendConfigDocument(cfg.BackendConfig))
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal backend config: %w", err)
	}
	auditPolicy, err := marshalAuditPolicy(cfg.Audit)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	var secretRefDocuments []secretRefDocument
	if cfg.SecretRefs != nil {
		secretRefDocuments = make([]secretRefDocument, len(cfg.SecretRefs))
	}
	for i, ref := range cfg.SecretRefs {
		secretRefDocuments[i] = secretRefDocument{Name: ref.Name, Version: ref.Version}
	}
	encodedSecretRefs, err := json.Marshal(secretRefDocuments)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal secret refs: %w", err)
	}
	bindings, err := json.Marshal(cfg.ChannelBinding)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal channel bindings: %w", err)
	}
	knowledgeBaseIDsValue := cfg.KnowledgeBaseIDs
	if knowledgeBaseIDsValue == nil {
		knowledgeBaseIDsValue = []string{}
	}
	knowledgeBaseIDs, err := json.Marshal(knowledgeBaseIDsValue)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, fmt.Errorf("marshal knowledge base ids: %w", err)
	}
	return modelConfig, toolPolicy, backendConfig, auditPolicy, encodedSecretRefs, bindings, knowledgeBaseIDs, nil
}

func unmarshalAppConfig(
	tenantID string,
	appID string,
	version string,
	modelConfig []byte,
	toolPolicy []byte,
	backendConfig []byte,
	auditPolicy []byte,
	secretRefs []byte,
	bindings []byte,
	knowledgeBaseIDs []byte,
) (tenant.AppConfig, error) {
	var modelDocument modelConfigDocument
	if err := json.Unmarshal(modelConfig, &modelDocument); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal model config: %w", err)
	}
	var toolDocument toolPolicyDocument
	if err := json.Unmarshal(toolPolicy, &toolDocument); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal tool policy: %w", err)
	}
	var backendDocument backendConfigDocument
	if err := json.Unmarshal(backendConfig, &backendDocument); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal backend config: %w", err)
	}
	audit, err := unmarshalAuditPolicy(auditPolicy)
	if err != nil {
		return tenant.AppConfig{}, err
	}
	var secretDocuments []secretRefDocument
	if err := json.Unmarshal(secretRefs, &secretDocuments); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal secret refs: %w", err)
	}
	var channelBinding []string
	if err := json.Unmarshal(bindings, &channelBinding); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal channel bindings: %w", err)
	}
	var knowledgeBaseIDList []string
	if err := json.Unmarshal(knowledgeBaseIDs, &knowledgeBaseIDList); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("unmarshal knowledge base ids: %w", err)
	}
	if len(knowledgeBaseIDList) == 0 {
		knowledgeBaseIDList = nil
	}
	var refs []tenant.SecretRef
	if secretDocuments != nil {
		refs = make([]tenant.SecretRef, len(secretDocuments))
	}
	for i, ref := range secretDocuments {
		refs[i] = tenant.SecretRef{Name: ref.Name, Version: ref.Version}
	}
	cfg := tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:   modelDocument.Provider,
			Model:      modelDocument.Model,
			APIKeyRef:  modelDocument.APIKeyRef.value(),
			Parameters: modelDocument.Parameters,
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:    toolDocument.VisibleTools,
			ExecutableTools: toolDocument.ExecutableTools,
		},
		BackendConfig:    backendDocument.value(),
		Audit:            audit,
		SecretRefs:       refs,
		ChannelBinding:   channelBinding,
		KnowledgeBaseIDs: knowledgeBaseIDList,
	}
	if err := cfg.Validate(); err != nil {
		return tenant.AppConfig{}, fmt.Errorf("stored app config: %w", err)
	}
	return cfg, nil
}

func newSecretRefDocument(ref tenant.SecretRef) secretRefDocument {
	return secretRefDocument{Name: ref.Name, Version: ref.Version}
}

func (d secretRefDocument) value() tenant.SecretRef {
	return tenant.SecretRef{Name: d.Name, Version: d.Version}
}

func marshalAuditPolicy(policy tenant.AuditPolicy) ([]byte, error) {
	encoded, err := json.Marshal(auditPolicyDocument{
		Enabled:       policy.Enabled,
		RetentionDays: policy.RetentionDays,
		RedactPII:     policy.RedactPII,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal audit policy: %w", err)
	}
	return encoded, nil
}

func unmarshalAuditPolicy(encoded []byte) (tenant.AuditPolicy, error) {
	var document auditPolicyDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return tenant.AuditPolicy{}, fmt.Errorf("unmarshal audit policy: %w", err)
	}
	policy := tenant.AuditPolicy{
		Enabled:       document.Enabled,
		RetentionDays: document.RetentionDays,
		RedactPII:     document.RedactPII,
	}
	if err := policy.Validate(); err != nil {
		return tenant.AuditPolicy{}, fmt.Errorf("stored audit policy: %w", err)
	}
	return policy, nil
}

func newBackendConfigDocument(config tenant.BackendConfig) backendConfigDocument {
	return backendConfigDocument{
		Name:      config.Name,
		Session:   newBackendRefDocument(config.Session),
		Memory:    newBackendRefDocument(config.Memory),
		Knowledge: newBackendRefDocument(config.Knowledge),
		Artifact:  newBackendRefDocument(config.Artifact),
	}
}

func newBackendRefDocument(ref tenant.BackendRef) backendRefDocument {
	return backendRefDocument{
		Kind:      ref.Kind,
		Provider:  ref.Provider,
		Name:      ref.Name,
		SecretRef: newSecretRefDocument(ref.SecretRef),
		DSNRef:    ref.DSNRef,
		Options:   ref.Options,
	}
}

func (d backendConfigDocument) value() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name:      d.Name,
		Session:   d.Session.value(),
		Memory:    d.Memory.value(),
		Knowledge: d.Knowledge.value(),
		Artifact:  d.Artifact.value(),
	}
}

func (d backendRefDocument) value() tenant.BackendRef {
	return tenant.BackendRef{
		Kind:      d.Kind,
		Provider:  d.Provider,
		Name:      d.Name,
		SecretRef: d.SecretRef.value(),
		DSNRef:    d.DSNRef,
		Options:   d.Options,
	}
}

// CreateChannelBinding inserts one verified IM account binding for a tenant
// application. Secret references are persisted as metadata only.
func (s *Store) CreateChannelBinding(ctx context.Context, binding channels.Binding) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("channel binding: %w", err)
	}
	tokenRef, signingSecretRef, secret, err := marshalBindingSecretRefs(binding)
	if err != nil {
		return err
	}
	if _, err := s.pool.Exec(
		ctx,
		`INSERT INTO platform.channel_binding (
    tenant_id,
    app_id,
    binding_id,
    channel,
    external_account,
    webhook_url,
    token_ref,
    signing_secret_ref,
    secret_ref,
    status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		binding.TenantID,
		binding.AppID,
		binding.BindingID,
		binding.Channel,
		binding.ExternalAccount,
		binding.WebhookURL,
		tokenRef,
		signingSecretRef,
		secret,
		binding.Status,
	); err != nil {
		return fmt.Errorf("create channel binding: %w", err)
	}
	return nil
}

// ResolveBinding returns one exact IM account binding in tenant application scope.
func (s *Store) ResolveBinding(
	ctx context.Context,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	if err := s.validate(); err != nil {
		return channels.Binding{}, err
	}
	if tenantID == "" {
		return channels.Binding{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return channels.Binding{}, errors.New("app_id is required")
	}
	if bindingID == "" {
		return channels.Binding{}, errors.New("binding_id is required")
	}
	var binding channels.Binding
	var tokenRef, signingSecretRef, secret []byte
	err := s.pool.QueryRow(
		ctx,
		`SELECT
    tenant_id,
    app_id,
    binding_id,
    channel,
    external_account,
    webhook_url,
    token_ref,
    signing_secret_ref,
    secret_ref,
    status
FROM platform.channel_binding
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		tenantID,
		appID,
		bindingID,
	).Scan(
		&binding.TenantID,
		&binding.AppID,
		&binding.BindingID,
		&binding.Channel,
		&binding.ExternalAccount,
		&binding.WebhookURL,
		&tokenRef,
		&signingSecretRef,
		&secret,
		&binding.Status,
	)
	if err != nil {
		return channels.Binding{}, resolveError("channel binding", err)
	}
	if err := unmarshalBindingSecretRefs(&binding, tokenRef, signingSecretRef, secret); err != nil {
		return channels.Binding{}, err
	}
	if err := binding.Validate(); err != nil {
		return channels.Binding{}, fmt.Errorf("stored channel binding: %w", err)
	}
	return binding, nil
}

func marshalBindingSecretRefs(binding channels.Binding) ([]byte, []byte, []byte, error) {
	tokenRef, err := json.Marshal(binding.TokenRef)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal channel token ref: %w", err)
	}
	signingSecretRef, err := json.Marshal(binding.SigningSecretRef)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal channel signing secret ref: %w", err)
	}
	secret, err := json.Marshal(binding.Secret)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal channel secret ref: %w", err)
	}
	return tokenRef, signingSecretRef, secret, nil
}

func unmarshalBindingSecretRefs(
	binding *channels.Binding,
	tokenRef, signingSecretRef, secret []byte,
) error {
	if binding == nil {
		return errors.New("channel binding is required")
	}
	if err := json.Unmarshal(tokenRef, &binding.TokenRef); err != nil {
		return fmt.Errorf("unmarshal channel token ref: %w", err)
	}
	if err := json.Unmarshal(signingSecretRef, &binding.SigningSecretRef); err != nil {
		return fmt.Errorf("unmarshal channel signing secret ref: %w", err)
	}
	if err := json.Unmarshal(secret, &binding.Secret); err != nil {
		return fmt.Errorf("unmarshal channel secret ref: %w", err)
	}
	return nil
}
