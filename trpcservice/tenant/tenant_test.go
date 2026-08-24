package tenant_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRuntimeContextValidateAndScopedKey(t *testing.T) {
	tc := tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "group/thread-1",
		SessionPrincipalID: "group-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	}
	if err := tc.Validate(); err != nil {
		t.Fatalf("validate runtime context: %v", err)
	}

	key, err := tc.Scope().Key("session", tc.SessionID)
	if err != nil {
		t.Fatalf("build scoped key: %v", err)
	}
	const want = "tenant:tenant-a:app:support:session:group%2Fthread-1"
	if key != want {
		t.Fatalf("scoped key = %q, want %q", key, want)
	}
}

func TestRuntimeContextValidateRejectsMissingTenant(t *testing.T) {
	tc := tenant.RuntimeContext{
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "session-1",
		SessionPrincipalID: "user-1",
	}
	if err := tc.Validate(); err == nil {
		t.Fatal("validate runtime context succeeded with missing tenant_id")
	}
}

func TestTenantAndAgentAppValidate(t *testing.T) {
	tnt := tenant.Tenant{
		ID:     "tenant-a",
		Name:   "Tenant A",
		Status: tenant.StatusActive,
		Audit:  tenant.AuditPolicy{Enabled: true, RetentionDays: 30},
	}
	if err := tnt.Validate(); err != nil {
		t.Fatalf("validate tenant: %v", err)
	}

	app := tenant.AgentApp{
		TenantID:            "tenant-a",
		AppID:               "support",
		Name:                "Support",
		ActiveConfigVersion: "v1",
	}
	if err := app.Validate(); err != nil {
		t.Fatalf("validate app: %v", err)
	}
}

func TestTenantValidateRejectsInvalidStatus(t *testing.T) {
	tnt := tenant.Tenant{
		ID:     "tenant-a",
		Name:   "Tenant A",
		Status: tenant.Status("DELETED"),
	}
	if err := tnt.Validate(); err == nil {
		t.Fatal("validate tenant succeeded with invalid status")
	}
}

func TestAppConfigValidateAndCloneCopiesNestedData(t *testing.T) {
	cfg := validAppConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate app config: %v", err)
	}

	cloned := cfg.Clone()
	cfg.Model.Parameters["temperature"] = "1"
	cfg.Tools.VisibleTools[0] = "mutated-visible"
	cfg.Tools.ExecutableTools[0] = "mutated-executable"
	cfg.BackendConfig.Session.Options["schema"] = "mutated"
	cfg.SecretRefs[0].Name = "mutated-secret"
	cfg.ChannelBinding[0] = "mutated-binding"

	if cloned.Model.Parameters["temperature"] != "0" {
		t.Fatalf("cloned model parameter = %q, want 0", cloned.Model.Parameters["temperature"])
	}
	if cloned.Tools.VisibleTools[0] != "search" {
		t.Fatalf("cloned visible tool = %q, want search", cloned.Tools.VisibleTools[0])
	}
	if cloned.Tools.ExecutableTools[0] != "search" {
		t.Fatalf("cloned executable tool = %q, want search", cloned.Tools.ExecutableTools[0])
	}
	if cloned.BackendConfig.Session.Options["schema"] != "agent" {
		t.Fatalf("cloned backend option = %q, want agent", cloned.BackendConfig.Session.Options["schema"])
	}
	if cloned.SecretRefs[0].Name != "model-api-key" {
		t.Fatalf("cloned secret name = %q, want model-api-key", cloned.SecretRefs[0].Name)
	}
	if cloned.ChannelBinding[0] != "binding-1" {
		t.Fatalf("cloned channel binding = %q, want binding-1", cloned.ChannelBinding[0])
	}
}

func TestAppConfigValidateAllowsNilOptionalCollections(t *testing.T) {
	cfg := tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider: "openai",
			Model:    "gpt-4.1-mini",
		},
		BackendConfig: tenant.BackendConfig{
			Name: "default",
			Session: tenant.BackendRef{
				Kind: tenant.BackendSQL,
				Name: "session-sql",
			},
		},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate app config with nil optionals: %v", err)
	}

	cloned := cfg.Clone()
	if cloned.Model.Parameters != nil {
		t.Fatalf("cloned parameters = %#v, want nil", cloned.Model.Parameters)
	}
	if cloned.Tools.VisibleTools != nil {
		t.Fatalf("cloned visible tools = %#v, want nil", cloned.Tools.VisibleTools)
	}
	if cloned.SecretRefs != nil {
		t.Fatalf("cloned secret refs = %#v, want nil", cloned.SecretRefs)
	}
}

func TestAppConfigValidateRejectsDuplicateTool(t *testing.T) {
	cfg := validAppConfig()
	cfg.Tools.VisibleTools = []string{"search", "search"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("validate app config succeeded with duplicate visible tools")
	}
}

func TestToolPolicyDefaultsToDeny(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:    []string{"search", "read"},
		ExecutableTools: []string{"search"},
	}

	if !policy.CanView("read") {
		t.Fatal("read tool is not visible")
	}
	if policy.CanExecute("read") {
		t.Fatal("read tool is executable without permission")
	}
	if !policy.CanExecute("search") {
		t.Fatal("search tool is not executable")
	}
	if policy.CanView("write") || policy.CanExecute("write") {
		t.Fatal("unknown tool is permitted")
	}
	if (tenant.ToolPolicy{}).CanView("") || (tenant.ToolPolicy{}).CanExecute("") {
		t.Fatal("zero policy permits an empty tool name")
	}
}

func TestBackendConfigValidateRejectsMissingSessionBackend(t *testing.T) {
	backend := validBackendConfig()
	backend.Session = tenant.BackendRef{}

	if err := backend.Validate(); err == nil {
		t.Fatal("validate backend_config succeeded with missing session backend")
	}
}

func TestBackendRefIsZeroChecksWholeRef(t *testing.T) {
	if !(tenant.BackendRef{}).IsZero() {
		t.Fatal("empty backend ref is not zero")
	}
	for _, ref := range []tenant.BackendRef{
		{Kind: tenant.BackendSQL},
		{Name: "session-sql"},
		{DSNRef: "secret-ref"},
		{Options: map[string]string{"schema": "agent"}},
	} {
		if ref.IsZero() {
			t.Fatalf("backend ref %#v is zero", ref)
		}
	}
}

func validAppConfig() tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:   "openai",
			Model:      "gpt-4.1-mini",
			Parameters: map[string]string{"temperature": "0"},
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:    []string{"search"},
			ExecutableTools: []string{"search"},
		},
		BackendConfig: validBackendConfig(),
		Audit: tenant.AuditPolicy{
			Enabled:       true,
			RetentionDays: 30,
			RedactPII:     true,
		},
		SecretRefs: []tenant.SecretRef{
			{Name: "model-api-key", Version: "v1"},
		},
		ChannelBinding: []string{"binding-1"},
	}
}

func validBackendConfig() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name: "default",
		Session: tenant.BackendRef{
			Kind:    tenant.BackendSQL,
			Name:    "session-sql",
			Options: map[string]string{"schema": "agent"},
		},
		Memory: tenant.BackendRef{
			Kind: tenant.BackendRedis,
			Name: "memory-redis",
		},
	}
}
