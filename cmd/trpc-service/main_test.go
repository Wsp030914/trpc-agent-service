package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestConfigFromEnvironmentRequiresExplicitWorkerIdentity(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:            string(roleWorker),
		envPostgresDSN:     "postgres://example",
		envRedisURL:        "redis://example:6379/0",
		envWorkerID:        "worker-a",
		envHTTPAddr:        "127.0.0.1:8081",
		envShutdownTimeout: "45s",
	}))
	if err != nil {
		t.Fatalf("config from environment: %v", err)
	}
	if config.Role != roleWorker || config.WorkerID != "worker-a" ||
		config.HTTPAddr != "127.0.0.1:8081" || config.ShutdownTimeout != 45*time.Second {
		t.Fatalf("config = %#v", config)
	}

	_, err = configFromEnvironment(environmentReader(map[string]string{
		envRole:        string(roleWorker),
		envPostgresDSN: "postgres://example",
		envRedisURL:    "redis://example:6379/0",
	}))
	if err == nil {
		t.Fatal("config without worker ID succeeded")
	}
}

func TestRenewLeaseCancelsWorkOnFailure(t *testing.T) {
	t.Parallel()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	want := errors.New("lease lost")
	go renewLease(runCtx, cancel, 10*time.Millisecond, done, func(context.Context) error {
		return want
	})

	select {
	case err := <-done:
		if !errors.Is(err, want) {
			t.Fatalf("renew error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("lease renewal did not finish")
	}
	if runCtx.Err() == nil {
		t.Fatal("lease renewal failure did not cancel work")
	}
}

func TestRenewLeaseBoundsRenewalCall(t *testing.T) {
	t.Parallel()
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go renewLease(runCtx, cancel, 30*time.Millisecond, done, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("renew error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded lease renewal did not finish")
	}
	if runCtx.Err() == nil {
		t.Fatal("bounded renewal failure did not cancel work")
	}
}

func TestConfigFromEnvironmentRequiresAdminTokenForGateway(t *testing.T) {
	_, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:        string(roleGateway),
		envPostgresDSN: "postgres://example",
		envRedisURL:    "redis://example:6379/0",
	}))
	if err == nil {
		t.Fatal("gateway configuration without admin token succeeded")
	}

	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:         string(roleGateway),
		envPostgresDSN:  "postgres://example",
		envRedisURL:     "redis://example:6379/0",
		envAdminToken:   "admin-token",
		envDispatcherID: "gateway-1",
	}))
	if err != nil || config.AdminToken != "admin-token" {
		t.Fatalf("gateway configuration = %#v, %v", config, err)
	}
}

func TestServiceHandlerRoutesGatewayIngress(t *testing.T) {
	handler := serviceHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("ingress path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/admin/v1/tenants" {
			t.Fatalf("admin path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	assertHTTPStatus(t, handler, "/v1/chat/completions", http.StatusNoContent)
	assertHTTPStatus(t, handler, "/admin/v1/tenants", http.StatusNoContent)
}

func TestAwaitWorkerExitReturnsAtShutdownDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	canceled := make(chan struct{})
	err, stopped := awaitWorkerExit(ctx, make(chan error), func() { close(canceled) })
	if stopped || !errors.Is(err, context.Canceled) {
		t.Fatalf("await worker exit = %v, %t", err, stopped)
	}
	select {
	case <-canceled:
	default:
		t.Fatal("worker cancellation was not requested")
	}
	shutdownErr := workerShutdownResult(nil, err, stopped)
	if !errors.Is(shutdownErr, errWorkerShutdownTimeout) {
		t.Fatalf("shutdown error = %v", shutdownErr)
	}
}

func TestAwaitWorkerExitReturnsWorkerResult(t *testing.T) {
	done := make(chan error, 1)
	want := errors.New("worker stopped")
	done <- want
	called := false
	err, stopped := awaitWorkerExit(context.Background(), done, func() { called = true })
	if !stopped || !errors.Is(err, want) || called {
		t.Fatalf("await worker exit = %v, %t, canceled=%t", err, stopped, called)
	}
}

func TestAwaitDataMigrationExitReturnsAtShutdownDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err, stopped := awaitDataMigrationExit(ctx, make(chan error))
	if stopped || !errors.Is(err, context.Canceled) {
		t.Fatalf("await data migration exit = %v, %t", err, stopped)
	}
	shutdownErr := dataMigrationShutdownResult(nil, err, stopped)
	if !errors.Is(shutdownErr, errWorkerShutdownTimeout) {
		t.Fatalf("shutdown error = %v", shutdownErr)
	}
}

func TestAwaitDataMigrationExitReturnsResult(t *testing.T) {
	done := make(chan error, 1)
	want := errors.New("migration stopped")
	done <- want
	err, stopped := awaitDataMigrationExit(context.Background(), done)
	if !stopped || !errors.Is(err, want) {
		t.Fatalf("await data migration exit = %v, %t", err, stopped)
	}
}

func TestEnvironmentSecretsAreScoped(t *testing.T) {
	scope := tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}
	ref := tenant.SecretRef{Name: "model-key", Version: "v1"}
	key := scopedSecretEnvironmentKey(scope, ref)
	sessionRef := tenant.SecretRef{Name: "session-dsn", Version: "v1"}
	sessionKey := scopedSecretEnvironmentKey(scope, sessionRef)
	provider := environmentSecretProvider{getenv: environmentReader(map[string]string{
		key:        "secret",
		sessionKey: "postgres://metadata",
	})}

	value, err := provider.ResolveSecret(context.Background(), scope, ref)
	if err != nil || value != "secret" {
		t.Fatalf("resolve scoped secret = %q, %v", value, err)
	}
	_, err = provider.ResolveSecret(context.Background(), tenant.Scope{TenantID: "tenant-b", AppID: "app-a"}, ref)
	if err == nil {
		t.Fatal("secret resolved for another tenant scope")
	}

	value, err = provider.ResolveSecret(context.Background(), scope, sessionRef)
	if err != nil || value != "postgres://metadata" {
		t.Fatalf("resolve session dsn = %q, %v", value, err)
	}
}

func TestEnvironmentTencentDBGatewayResolverUsesBackendName(t *testing.T) {
	resolver := environmentTencentDBGatewayResolver{getenv: environmentReader(map[string]string{
		envTencentDBGateways: `{"memory-tenant-a":"https://memory-a.example"}`,
	})}
	value, err := resolver.ResolveTencentDBGateway(context.Background(), "memory-tenant-a")
	if err != nil || value != "https://memory-a.example" {
		t.Fatalf("resolve gateway = %q, %v", value, err)
	}
	if _, err := resolver.ResolveTencentDBGateway(context.Background(), "memory-tenant-b"); err == nil {
		t.Fatal("resolved an unconfigured TencentDB gateway")
	}
}

func environmentReader(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func assertHTTPStatus(t *testing.T, handler http.Handler, path string, want int) {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != want {
		t.Fatalf("GET %s status = %d, want %d", path, response.Code, want)
	}
}
