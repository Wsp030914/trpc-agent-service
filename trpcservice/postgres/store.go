package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const rollbackTimeout = 5 * time.Second

var (
	// ErrNotFound means the requested platform record does not exist.
	ErrNotFound = errors.New("postgres record not found")
)

var (
	_ auth.CredentialStore        = (*Store)(nil)
	_ auth.Directory              = (*Store)(nil)
	_ config.Resolver             = (*Store)(nil)
	_ gateway.PublicRouteResolver = (*Store)(nil)
	_ gateway.Admitter            = (*Store)(nil)
)

// Store is the concrete PostgreSQL implementation for platform-owned records.
// Callers should depend on capability interfaces from the consuming package,
// such as auth.CredentialStore, auth.Directory, or config.Resolver, rather
// than on a cross-backend Store interface. The caller owns the supplied pool
// and must close it after all Store operations have stopped. Store is safe for
// concurrent use.
type Store struct {
	pool           *pgxpool.Pool
	identityMapper *IdentityMapper
}

// StoreOption configures a Store before it is used concurrently.
type StoreOption func(*Store) error

// WithChannelIdentityMapping configures the scoped Identity and Conversation
// mapper used by channel admission. The supplied implementations must use
// the platform's existing scoped secret infrastructure.
func WithChannelIdentityMapping(
	hasher channels.ExternalIDHasher,
	protector channels.TargetProtector,
	acceptedKeyVersions []string,
) StoreOption {
	return func(store *Store) error {
		mapper, err := NewIdentityMapper(store, hasher, protector, acceptedKeyVersions)
		if err != nil {
			return err
		}
		store.identityMapper = mapper
		return nil
	}
}

// New creates a Store using a caller-owned PostgreSQL pool.
func New(pool *pgxpool.Pool, options ...StoreOption) (*Store, error) {
	if pool == nil {
		return nil, errors.New("postgres pool is required")
	}
	store := &Store{pool: pool}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("postgres store option is required")
		}
		if err := option(store); err != nil {
			return nil, fmt.Errorf("configure postgres store: %w", err)
		}
	}
	return store, nil
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

func (s *Store) validate() error {
	if s == nil || s.pool == nil {
		return errors.New("postgres store is not initialized")
	}
	return nil
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), rollbackTimeout)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		// Callers already return the operation error; retain rollback failures in
		// the process log instead of silently discarding them.
		log.Printf("postgres transaction rollback failed: %s", platformlog.SafeError(err))
	}
}

func resolveError(entity string, err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%s: %w", entity, ErrNotFound)
	}
	return fmt.Errorf("resolve %s: %w", entity, err)
}

// CreateChannelBinding inserts one tenant-owned IM account binding for a tenant
// application. Missing route metadata is generated for lower-level callers;
// an explicitly supplied route must have the format emitted by
// channels.NewPublicRouteID.
func (s *Store) CreateChannelBinding(ctx context.Context, binding channels.Binding) error {
	if err := s.validate(); err != nil {
		return err
	}
	if binding.PublicRouteID == "" {
		publicRouteID, err := channels.NewPublicRouteID()
		if err != nil {
			return err
		}
		binding.PublicRouteID = publicRouteID
	} else if err := channels.ValidateGeneratedPublicRouteID(binding.PublicRouteID); err != nil {
		return fmt.Errorf("channel binding public route: %w", err)
	}
	if binding.BindingRevision == 0 {
		binding.BindingRevision = 1
	}
	if binding.BindingRevision != 1 {
		return errors.New("binding_revision must be 1 when creating a channel binding")
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
    public_route_id,
    binding_revision,
    status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		binding.TenantID,
		binding.AppID,
		binding.BindingID,
		binding.Channel,
		binding.ExternalAccount,
		binding.WebhookURL,
		tokenRef,
		signingSecretRef,
		secret,
		binding.PublicRouteID,
		binding.BindingRevision,
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
	binding, err := scanChannelBinding(s.pool.QueryRow(
		ctx,
		channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		tenantID,
		appID,
		bindingID,
	))
	if err != nil {
		return channels.Binding{}, resolveError("channel binding", err)
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
