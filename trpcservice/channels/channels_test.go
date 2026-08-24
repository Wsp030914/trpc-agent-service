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
		Status: channels.BindingActive,
	}
}
