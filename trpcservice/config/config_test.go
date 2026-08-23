package config_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestStaticResolverReturnsExactConfigAndCopiesMutableData(t *testing.T) {
	cfg := testAppConfig("tenant-a", "support", "v1")
	resolver, err := config.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("new static resolver: %v", err)
	}

	cfg.Model.Parameters["temperature"] = "1"
	cfg.Tools.VisibleTools[0] = "mutated"
	cfg.Backend.Session.Options["schema"] = "mutated"

	resolved, err := resolver.ResolveAppConfig(context.Background(), "tenant-a", "support", "v1")
	if err != nil {
		t.Fatalf("resolve app config: %v", err)
	}
	if got := resolved.Model.Parameters["temperature"]; got != "0" {
		t.Fatalf("model temperature = %q, want 0", got)
	}
	if got := resolved.Tools.VisibleTools[0]; got != "search" {
		t.Fatalf("visible tool = %q, want search", got)
	}
	if got := resolved.Backend.Session.Options["schema"]; got != "agent" {
		t.Fatalf("session schema = %q, want agent", got)
	}

	resolved.Model.Parameters["temperature"] = "2"
	again, err := resolver.ResolveAppConfig(context.Background(), "tenant-a", "support", "v1")
	if err != nil {
		t.Fatalf("resolve app config again: %v", err)
	}
	if got := again.Model.Parameters["temperature"]; got != "0" {
		t.Fatalf("model temperature after caller mutation = %q, want 0", got)
	}
}

func TestStaticResolverIsolatesTenantAppAndVersion(t *testing.T) {
	resolver, err := config.NewStaticResolver(
		testAppConfig("tenant-a", "support", "v1"),
		testAppConfig("tenant-a", "support", "v2"),
		testAppConfig("tenant-b", "support", "v1"),
		testAppConfig("tenant-a", "sales", "v1"),
	)
	if err != nil {
		t.Fatalf("new static resolver: %v", err)
	}

	tests := []struct {
		name     string
		tenantID string
		appID    string
		version  string
	}{
		{name: "tenant", tenantID: "tenant-b", appID: "support", version: "v1"},
		{name: "app", tenantID: "tenant-a", appID: "sales", version: "v1"},
		{name: "version", tenantID: "tenant-a", appID: "support", version: "v2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := resolver.ResolveAppConfig(
				context.Background(),
				tt.tenantID,
				tt.appID,
				tt.version,
			)
			if err != nil {
				t.Fatalf("resolve app config: %v", err)
			}
			if cfg.TenantID != tt.tenantID || cfg.AppID != tt.appID || cfg.Version != tt.version {
				t.Fatalf(
					"resolved scope = %q/%q/%q, want %q/%q/%q",
					cfg.TenantID,
					cfg.AppID,
					cfg.Version,
					tt.tenantID,
					tt.appID,
					tt.version,
				)
			}
		})
	}
}

func TestNewStaticResolverRejectsInvalidOrDuplicateConfig(t *testing.T) {
	invalid := testAppConfig("tenant-a", "support", "v1")
	invalid.Model.Model = ""
	if _, err := config.NewStaticResolver(invalid); err == nil {
		t.Fatal("new static resolver succeeded with invalid config")
	}

	cfg := testAppConfig("tenant-a", "support", "v1")
	if _, err := config.NewStaticResolver(cfg, cfg); err == nil {
		t.Fatal("new static resolver succeeded with duplicate config")
	}
}

func TestStaticResolverRejectsIncompleteOrUnknownScope(t *testing.T) {
	resolver, err := config.NewStaticResolver(testAppConfig("tenant-a", "support", "v1"))
	if err != nil {
		t.Fatalf("new static resolver: %v", err)
	}

	tests := []struct {
		name     string
		tenantID string
		appID    string
		version  string
	}{
		{name: "missing tenant", appID: "support", version: "v1"},
		{name: "missing app", tenantID: "tenant-a", version: "v1"},
		{name: "missing version", tenantID: "tenant-a", appID: "support"},
		{name: "unknown config", tenantID: "tenant-b", appID: "support", version: "v1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := resolver.ResolveAppConfig(
				context.Background(),
				tt.tenantID,
				tt.appID,
				tt.version,
			); err == nil {
				t.Fatal("resolve app config succeeded with invalid scope")
			}
		})
	}
}

func testAppConfig(tenantID, appID, version string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:   "openai",
			Model:      "gpt-4.1-mini",
			Parameters: map[string]string{"temperature": "0"},
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:    []string{"search"},
			ExecutableTools: []string{"search"},
		},
		Backend: tenant.BackendProfile{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:    tenant.BackendSQL,
				Name:    "session-sql",
				Options: map[string]string{"schema": "agent"},
			},
		},
	}
}
