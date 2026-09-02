package channels_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestBindingValidateRequiresTrustedChannelConfig(t *testing.T) {
	binding := validBinding()
	if err := binding.Validate(); err != nil {
		t.Fatalf("validate binding: %v", err)
	}
	if scope := binding.Scope(); scope.TenantID != "tenant-a" || scope.AppID != "support" {
		t.Fatalf("scope = %#v, want tenant-a/support", scope)
	}
}

func TestBindingValidateAcceptsFeishu(t *testing.T) {
	binding := validBinding()
	binding.Channel = channels.ChannelFeishu
	binding.ExternalAccount = "feishu-app-1"
	binding.ExternalAccountScope = "feishu-tenant-1"
	binding.WebhookURL = "https://example.com/im/feishu/binding-1"
	binding.TokenRef = tenant.SecretRef{Name: "feishu-token", Version: "v1"}
	binding.SigningSecretRef = tenant.SecretRef{Name: "feishu-signing-secret", Version: "v1"}
	binding.Secret = tenant.SecretRef{Name: "feishu-app-secret", Version: "v1"}

	if err := binding.Validate(); err != nil {
		t.Fatalf("validate Feishu binding: %v", err)
	}
}

func TestBindingValidateRequiresFeishuOutboundSecret(t *testing.T) {
	binding := validBinding()
	binding.Channel = channels.ChannelFeishu
	binding.ExternalAccountScope = "feishu-tenant-1"
	if err := binding.Validate(); err == nil {
		t.Fatal("validate Feishu binding succeeded without outbound app secret")
	}
}

func TestBindingValidateRequiresFeishuExternalAccountScope(t *testing.T) {
	binding := validBinding()
	binding.Channel = channels.ChannelFeishu
	if err := binding.Validate(); err == nil {
		t.Fatal("validate Feishu binding succeeded without external account scope")
	}
}

func TestBindingValidateRejectsIncompleteConfig(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*channels.Binding)
	}{
		{name: "tenant", mutate: func(b *channels.Binding) { b.TenantID = "" }},
		{name: "app", mutate: func(b *channels.Binding) { b.AppID = "" }},
		{name: "binding", mutate: func(b *channels.Binding) { b.BindingID = "" }},
		{name: "channel", mutate: func(b *channels.Binding) { b.Channel = channels.Channel("slack") }},
		{name: "external account", mutate: func(b *channels.Binding) { b.ExternalAccount = "" }},
		{name: "webhook", mutate: func(b *channels.Binding) { b.WebhookURL = "" }},
		{name: "token", mutate: func(b *channels.Binding) { b.TokenRef = tenant.SecretRef{} }},
		{name: "signing secret", mutate: func(b *channels.Binding) { b.SigningSecretRef = tenant.SecretRef{} }},
		{name: "status", mutate: func(b *channels.Binding) { b.Status = channels.BindingStatus("DELETED") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := validBinding()
			tt.mutate(&binding)
			if err := binding.Validate(); err == nil {
				t.Fatal("validate binding succeeded with invalid config")
			}
		})
	}
}

func TestBindingValidateRequiresPublicRouteSnapshot(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*channels.Binding)
	}{
		{name: "route", mutate: func(b *channels.Binding) { b.PublicRouteID = "" }},
		{name: "revision", mutate: func(b *channels.Binding) { b.BindingRevision = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := validBinding()
			tt.mutate(&binding)
			if err := binding.Validate(); err == nil {
				t.Fatal("validate binding succeeded without route snapshot")
			}
		})
	}
}

func TestNewPublicRouteIDIsURLSafeAndUnpredictable(t *testing.T) {
	first, err := channels.NewPublicRouteID()
	if err != nil {
		t.Fatalf("generate first public route: %v", err)
	}
	second, err := channels.NewPublicRouteID()
	if err != nil {
		t.Fatalf("generate second public route: %v", err)
	}
	if first == second {
		t.Fatal("generated public routes are duplicated")
	}
	for _, route := range []string{first, second} {
		if err := channels.ValidatePublicRouteID(route); err != nil {
			t.Fatalf("validate generated public route %q: %v", route, err)
		}
	}
}

func validBinding() channels.Binding {
	return channels.Binding{
		TenantID:        "tenant-a",
		AppID:           "support",
		BindingID:       "binding-1",
		Channel:         channels.ChannelWeCom,
		ExternalAccount: "corp-agent-1",
		WebhookURL:      "https://example.com/im/wecom/binding-1",
		TokenRef: tenant.SecretRef{
			Name:    "wecom-token",
			Version: "v1",
		},
		SigningSecretRef: tenant.SecretRef{
			Name:    "wecom-signing-secret",
			Version: "v1",
		},
		PublicRouteID:   "route-binding-1",
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}
