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

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
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

// CreateChannelBinding validates and persists an IM account binding owned by
// one tenant application. It does not enable an IM callback by itself.
func (a API) CreateChannelBinding(ctx context.Context, binding channels.Binding) error {
	if err := binding.Validate(); err != nil {
		return err
	}
	repository, err := a.repository()
	if err != nil {
		return err
	}
	if err := repository.CreateChannelBinding(ctx, binding); err != nil {
		return fmt.Errorf("create channel binding: %w", err)
	}
	return nil
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
	if err := config.ValidateAppConfigBindings(ctx, cfg, a.Bindings); err != nil {
		return fmt.Errorf("channel bindings: %w", err)
	}
	return nil
}

// CreateTenant validates and persists a new tenant control-plane record.
func (a API) CreateTenant(ctx context.Context, value tenant.Tenant) error {
	if err := value.Validate(); err != nil {
		return err
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
		return err
	}
	if err := a.ValidateAppConfig(ctx, initial); err != nil {
		return fmt.Errorf("initial app config: %w", err)
	}
	if app.TenantID != initial.TenantID || app.AppID != initial.AppID {
		return errors.New("initial app config does not match agent app scope")
	}
	if app.ActiveConfigVersion != initial.Version {
		return errors.New("active_config_version does not match initial app config")
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
		return err
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
		return err
	}
	if version == "" {
		return errors.New("config version is required")
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
		return IssuedCredential{}, err
	}
	if !expiresAt.IsZero() && !expiresAt.After(time.Now()) {
		return IssuedCredential{}, errors.New("credential expiry must be in the future")
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
		return err
	}
	if credentialID == "" {
		return errors.New("credential_id is required")
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

func (a API) repository() (Repository, error) {
	if a.Repository == nil {
		return nil, errors.New("admin repository is required")
	}
	return a.Repository, nil
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
