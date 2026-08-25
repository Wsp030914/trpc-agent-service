package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestStaticResolverReturnsScopedHandlesAndCopiesBackendConfig(t *testing.T) {
	resolver := storage.StaticResolver{}
	tc := testRuntimeContext()
	backend := testBackendConfig()

	handles, err := resolver.Resolve(context.Background(), tc, backend)
	if err != nil {
		t.Fatalf("resolve backend_config: %v", err)
	}
	backend.Session.Options["schema"] = "mutated"
	handles.Session.Ref.Options["schema"] = "mutated"

	again, err := resolver.Resolve(context.Background(), tc, testBackendConfig())
	if err != nil {
		t.Fatalf("resolve backend_config again: %v", err)
	}
	if again.Scope.TenantID != "tenant-a" || again.Scope.AppID != "support" {
		t.Fatalf("scope = %#v, want tenant-a/support", again.Scope)
	}
	if again.BackendConfigName != "shared" {
		t.Fatalf("backend_config name = %q, want shared", again.BackendConfigName)
	}
	if got := again.Session.Ref.Options["schema"]; got != "agent" {
		t.Fatalf("session schema = %q, want agent", got)
	}
	if again.Memory.Ref.Name != "memory-redis" {
		t.Fatalf("memory backend name = %q, want memory-redis", again.Memory.Ref.Name)
	}
	if !again.Knowledge.IsZero() {
		t.Fatalf("knowledge handle = %#v, want zero", again.Knowledge)
	}
	key, err := again.Session.Key(tc.SessionPrincipalID, tc.SessionID)
	if err != nil {
		t.Fatalf("session key: %v", err)
	}
	const want = "tenant:tenant-a:app:support:session:principal-1:session-1"
	if key != want {
		t.Fatalf("session key = %q, want %q", key, want)
	}
}

func TestHandlesValidateRejectsScopeOrBackendMismatch(t *testing.T) {
	handles, err := (storage.StaticResolver{}).Resolve(
		context.Background(),
		testRuntimeContext(),
		testBackendConfig(),
	)
	if err != nil {
		t.Fatalf("resolve backend_config: %v", err)
	}

	wrongScope := handles
	wrongScope.Scope = tenant.Scope{TenantID: "tenant-b", AppID: "support"}
	if err := wrongScope.Validate(testRuntimeContext(), testBackendConfig()); err == nil {
		t.Fatal("validate handles succeeded with wrong scope")
	}

	wrongBackend := handles
	wrongBackend.Session.Ref.Name = "other-session-sql"
	if err := wrongBackend.Validate(testRuntimeContext(), testBackendConfig()); err == nil {
		t.Fatal("validate handles succeeded with wrong backend ref")
	}
}

func TestHandleIsZeroChecksWholeBackendRef(t *testing.T) {
	handle := storage.Handle{
		Ref: tenant.BackendRef{
			DSNRef: "secret-ref",
		},
	}
	if handle.IsZero() {
		t.Fatal("handle with dsn ref is zero")
	}

	handle = storage.Handle{
		Ref: tenant.BackendRef{
			Options: map[string]string{"schema": "agent"},
		},
	}
	if handle.IsZero() {
		t.Fatal("handle with backend options is zero")
	}
}

func TestStaticResolverRejectsInvalidContextOrBackend(t *testing.T) {
	resolver := storage.StaticResolver{}
	if _, err := resolver.Resolve(context.Background(), tenant.RuntimeContext{}, testBackendConfig()); err == nil {
		t.Fatal("resolve backend_config succeeded with invalid runtime context")
	}

	backend := testBackendConfig()
	backend.Session = tenant.BackendRef{}
	if _, err := resolver.Resolve(context.Background(), testRuntimeContext(), backend); err == nil {
		t.Fatal("resolve backend_config succeeded with invalid backend_config")
	}
}

func TestStaticResolverInMemoryPolicyIsOptIn(t *testing.T) {
	backend := testBackendConfig()
	backend.Session = tenant.BackendRef{
		Kind: tenant.BackendInMemory,
		Name: "session-local",
	}

	if _, err := (storage.StaticResolver{}).Resolve(context.Background(), testRuntimeContext(), backend); !errors.Is(err, storage.ErrInMemoryDisabled) {
		t.Fatalf("resolve inmemory backend error = %v, want ErrInMemoryDisabled", err)
	}
	if _, err := (storage.StaticResolver{AllowInMemory: true}).Resolve(context.Background(), testRuntimeContext(), backend); err != nil {
		t.Fatalf("resolve inmemory backend with opt-in: %v", err)
	}
}

func TestStaticResolverHonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := (storage.StaticResolver{}).Resolve(ctx, testRuntimeContext(), testBackendConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("resolve canceled context error = %v, want context.Canceled", err)
	}
}

func testBackendConfig() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name: "shared",
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

func testRuntimeContext() tenant.RuntimeContext {
	return tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "session-1",
		SessionPrincipalID: "principal-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	}
}
