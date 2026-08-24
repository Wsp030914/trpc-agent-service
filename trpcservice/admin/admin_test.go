package admin_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
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

func testAppConfig() tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider: "openai",
			Model:    "gpt-4.1-mini",
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind: tenant.BackendSQL,
				Name: "session-sql",
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
		Status: channels.BindingActive,
	}
}
