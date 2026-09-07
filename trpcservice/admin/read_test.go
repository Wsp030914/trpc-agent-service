package admin

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestSanitizeStringMapOmitsOpaqueConfigValues(t *testing.T) {
	got := sanitizeStringMap(map[string]string{
		"endpoint": "postgres://user:raw-secret@db.internal/app",
		"host":     "db.internal",
	})
	if got != nil {
		t.Fatalf("opaque config values = %#v, want omitted", got)
	}
}

func TestSanitizeAppConfigKeepsOnlyNonSecretRuntimeOptions(t *testing.T) {
	config := tenant.AppConfig{
		Model: tenant.ModelConfig{Parameters: map[string]string{
			"base_url": "https://api.example.test/v1",
			"api_key":  "should-not-return",
		}},
		BackendConfig: tenant.BackendConfig{
			Session:   tenant.BackendRef{Options: map[string]string{"schema": "agent", "dsn": "secret"}},
			Knowledge: tenant.BackendRef{Options: map[string]string{"index_generation": "g1", "api_key": "secret"}},
		},
	}
	got := sanitizeAppConfig(config)
	if got.Model.Parameters["base_url"] != "https://api.example.test/v1" || got.Model.Parameters["api_key"] != "" {
		t.Fatalf("model parameters = %#v", got.Model.Parameters)
	}
	if got.BackendConfig.Session.Options["schema"] != "agent" || got.BackendConfig.Session.Options["dsn"] != "" {
		t.Fatalf("session options = %#v", got.BackendConfig.Session.Options)
	}
	if got.BackendConfig.Knowledge.Options["index_generation"] != "g1" || got.BackendConfig.Knowledge.Options["api_key"] != "" {
		t.Fatalf("knowledge options = %#v", got.BackendConfig.Knowledge.Options)
	}
}
