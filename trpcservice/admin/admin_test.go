package admin_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAPIValidateAppConfigChecksChannelBindings(t *testing.T) {
	cfg := testAppConfig()
	binding := testBinding()
	bindings, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatalf("new binding resolver: %v", err)
	}
	api := admin.API{Bindings: bindings}

	if err := api.ValidateAppConfig(context.Background(), cfg); err != nil {
		t.Fatalf("validate app config: %v", err)
	}
}

func TestAPIValidateAppConfigRejectsMissingChannelBinding(t *testing.T) {
	api := admin.API{}
	if err := api.ValidateAppConfig(context.Background(), testAppConfig()); err == nil {
		t.Fatal("validate app config succeeded without binding resolver")
	}
}

func TestAPIValidateAppConfigAllowsKnowledgeWithoutArtifact(t *testing.T) {
	cfg := testAppConfig()
	cfg.BackendConfig.Knowledge = tenant.BackendRef{
		Kind:     tenant.BackendVector,
		Provider: "qdrant",
		Name:     "shared-qdrant",
		Options: map[string]string{
			"embedding_model":      "text-embedding-3-small",
			"embedding_dimensions": "1536",
			"embedding_profile":    "text-embedding-3-small",
			"index_generation":     "g1",
		},
	}
	cfg.KnowledgeBaseIDs = []string{"kb-1"}
	bindings, err := config.NewStaticBindingResolver(testBinding())
	if err != nil {
		t.Fatalf("new binding resolver: %v", err)
	}
	if err := (admin.API{Bindings: bindings}).ValidateAppConfig(context.Background(), cfg); err != nil {
		t.Fatalf("validate app config with Knowledge but no Artifact COS: %v", err)
	}
}

func TestAPIRejectsCallerSuppliedChannelBindingRoute(t *testing.T) {
	api := admin.API{Repository: &recordingRepository{}}
	binding := testBinding()
	if _, err := api.ProvisionChannelBinding(context.Background(), binding); err == nil {
		t.Fatal("provision channel binding accepted caller-supplied route")
	}
}

func TestAPIIssueCredentialRejectsExpiredCredential(t *testing.T) {
	api := admin.API{Repository: &recordingRepository{}}
	_, err := api.IssueCredential(
		context.Background(),
		tenant.Scope{TenantID: "tenant-a", AppID: "support"},
		time.Now().Add(-time.Second),
	)
	if err == nil {
		t.Fatal("issue credential succeeded with expired time")
	}
}

func testAppConfig() tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  "openai",
			APIKeyRef: tenant.SecretRef{Name: "model-key"},
			Model:     "gpt-4.1-mini",
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-sql",
			},
		},
		ChannelBinding: []string{"binding-1"},
	}
}

func testBinding() channels.Binding {
	return channels.Binding{
		TenantID:        "tenant-a",
		AppID:           "support",
		BindingID:       "binding-1",
		Channel:         channels.ChannelWeCom,
		ExternalAccount: "corp-agent-1",
		WebhookURL:      "https://example.com/im/wecom/binding-1",
		TokenRef: tenant.SecretRef{
			Name: "wecom-token",
		},
		SigningSecretRef: tenant.SecretRef{
			Name: "wecom-signing-secret",
		},
		PublicRouteID:   "route-binding-1",
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}

type recordingRepository struct {
	tenant              tenant.Tenant
	app                 tenant.AgentApp
	binding             channels.Binding
	initial             tenant.AppConfig
	published           tenant.AppConfig
	activatedTenantID   string
	activatedAppID      string
	activatedVersion    string
	digest              auth.APIKeyDigest
	credential          auth.Credential
	revokedTenantID     string
	revokedAppID        string
	revokedCredentialID string
	auditEvents         []platformaudit.Event
	auditTenantID       string
	auditAppID          string
	auditLimit          int
}

func (r *recordingRepository) CreateTenant(_ context.Context, value tenant.Tenant) error {
	r.tenant = value
	return nil
}

func (r *recordingRepository) CreateAgentApp(
	_ context.Context,
	app tenant.AgentApp,
	initial tenant.AppConfig,
) error {
	r.app = app
	r.initial = initial
	return nil
}

func (r *recordingRepository) CreateChannelBinding(_ context.Context, binding channels.Binding) error {
	r.binding = binding
	return nil
}

func (r *recordingRepository) ResolveBinding(
	_ context.Context,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	if r.binding.TenantID != tenantID || r.binding.AppID != appID || r.binding.BindingID != bindingID {
		return channels.Binding{}, errors.New("channel binding not found")
	}
	return r.binding, nil
}

func (r *recordingRepository) InsertAppConfigVersion(_ context.Context, cfg tenant.AppConfig) error {
	r.published = cfg
	return nil
}

func (r *recordingRepository) ActivateAppConfig(
	_ context.Context,
	tenantID, appID, version string,
) error {
	r.activatedTenantID = tenantID
	r.activatedAppID = appID
	r.activatedVersion = version
	return nil
}

func (r *recordingRepository) CreateCredential(
	_ context.Context,
	digest auth.APIKeyDigest,
	credential auth.Credential,
) error {
	r.digest = digest
	r.credential = credential
	return nil
}

func (r *recordingRepository) RevokeCredential(
	_ context.Context,
	tenantID, appID, credentialID string,
) error {
	r.revokedTenantID = tenantID
	r.revokedAppID = appID
	r.revokedCredentialID = credentialID
	return nil
}

func (r *recordingRepository) ListAuditEvents(
	_ context.Context,
	tenantID, appID string,
	limit int,
) ([]platformaudit.Event, error) {
	r.auditTenantID = tenantID
	r.auditAppID = appID
	r.auditLimit = limit
	result := make([]platformaudit.Event, 0)
	for _, event := range r.auditEvents {
		if event.TenantID == tenantID && event.AppID == appID {
			result = append(result, event)
		}
	}
	return result, nil
}
