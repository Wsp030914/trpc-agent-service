package storage_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestStaticResolverValidatesAndCopiesBackendProfile(t *testing.T) {
	resolver := storage.StaticResolver{Backend: testBackendProfile()}
	tc := testRuntimeContext()

	backend, err := resolver.ResolveBackend(context.Background(), tc)
	if err != nil {
		t.Fatalf("resolve backend: %v", err)
	}
	backend.Session.Options["schema"] = "mutated"

	again, err := resolver.ResolveBackend(context.Background(), tc)
	if err != nil {
		t.Fatalf("resolve backend again: %v", err)
	}
	if got := again.Session.Options["schema"]; got != "agent" {
		t.Fatalf("session schema after caller mutation = %q, want agent", got)
	}
}

func TestStaticResolverRejectsInvalidContextOrBackend(t *testing.T) {
	resolver := storage.StaticResolver{Backend: testBackendProfile()}
	if _, err := resolver.ResolveBackend(context.Background(), tenant.RuntimeContext{}); err == nil {
		t.Fatal("resolve backend succeeded with invalid runtime context")
	}

	resolver.Backend.Session = tenant.BackendRef{}
	if _, err := resolver.ResolveBackend(context.Background(), testRuntimeContext()); err == nil {
		t.Fatal("resolve backend succeeded with invalid backend profile")
	}
}

func testBackendProfile() tenant.BackendProfile {
	return tenant.BackendProfile{
		Name: "shared",
		Session: tenant.BackendRef{
			Kind:    tenant.BackendSQL,
			Name:    "session-sql",
			Options: map[string]string{"schema": "agent"},
		},
	}
}

func testRuntimeContext() tenant.RuntimeContext {
	return tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "session-1",
		SessionPrincipalID: "principal-1",
	}
}
