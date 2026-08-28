package secret

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestSecretModelAPIKeyResolverUsesConfiguredScopedReference(t *testing.T) {
	provider := &recordingProvider{value: "model-key"}
	resolver, err := NewSecretModelAPIKeyResolver(provider)
	if err != nil {
		t.Fatalf("new secret model api key resolver: %v", err)
	}
	exec := worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID: "tenant-a", AppID: "app-a", ConfigVersion: "v1",
			SessionID: "session-1", SessionPrincipalID: "principal-1", UserID: "user-1",
		},
		Config: tenant.AppConfig{
			TenantID: "tenant-a", AppID: "app-a", Version: "v1",
			Model: tenant.ModelConfig{
				Provider: "openai", Model: "gpt-test",
				APIKeyRef: tenant.SecretRef{Name: "model-api-key", Version: "v2"},
			},
		},
	}

	value, err := resolver.ResolveModelAPIKey(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve model api key: %v", err)
	}
	if value != "model-key" {
		t.Fatalf("model api key = %q", value)
	}
	if provider.scope != exec.Tenant.Scope() || provider.ref != exec.Config.Model.APIKeyRef {
		t.Fatalf("secret request = %#v %#v", provider.scope, provider.ref)
	}
}

type recordingProvider struct {
	scope tenant.Scope
	ref   tenant.SecretRef
	value string
}

func (p *recordingProvider) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	p.scope = scope
	p.ref = ref
	return p.value, nil
}
