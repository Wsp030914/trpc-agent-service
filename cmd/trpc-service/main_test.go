package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestConfigFromEnvironmentRequiresExplicitWorkerIdentity(t *testing.T) {
	config, err := configFromEnvironment(environmentReader(map[string]string{
		envRole:            string(roleWorker),
		envPostgresDSN:     "postgres://example",
		envRedisURL:        "redis://example:6379/0",
		envWorkerID:        "worker-a",
		envHealthAddr:      "127.0.0.1:8081",
		envShutdownTimeout: "45s",
	}))
	if err != nil {
		t.Fatalf("config from environment: %v", err)
	}
	if config.Role != roleWorker || config.WorkerID != "worker-a" ||
		config.HealthAddr != "127.0.0.1:8081" || config.ShutdownTimeout != 45*time.Second {
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

func TestHealthHandlerReadinessTracksDependencies(t *testing.T) {
	state := &healthState{}
	handler := healthHandler(state)

	assertHTTPStatus(t, handler, "/livez", http.StatusOK)
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)

	state.ready.Store(true)
	assertHTTPStatus(t, handler, "/readyz", http.StatusOK)
	state.check = func(context.Context) error { return errors.New("postgres unavailable") }
	assertHTTPStatus(t, handler, "/readyz", http.StatusServiceUnavailable)
}

func TestReadinessCheckChecksPostgresAndRedis(t *testing.T) {
	postgres := healthChecker(func(context.Context) error { return nil })
	redis := healthChecker(func(context.Context) error { return errors.New("redis unavailable") })
	if err := readinessCheck(postgres, redis)(context.Background()); err == nil {
		t.Fatal("readiness check succeeded with unavailable redis")
	}
}

func TestServiceHandlerRoutesGatewayIngress(t *testing.T) {
	state := &healthState{}
	state.ready.Store(true)
	handler := serviceHandler(state, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	assertHTTPStatus(t, handler, "/readyz", http.StatusOK)
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

func TestEnvironmentSecretsAreScoped(t *testing.T) {
	scope := tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}
	ref := tenant.SecretRef{Name: "model-key", Version: "v1"}
	key := scopedSecretEnvironmentKey(scope, ref)
	provider := environmentSecretProvider{getenv: environmentReader(map[string]string{key: "secret"})}

	value, err := provider.ResolveSecret(context.Background(), scope, ref)
	if err != nil || value != "secret" {
		t.Fatalf("resolve scoped secret = %q, %v", value, err)
	}
	_, err = provider.ResolveSecret(context.Background(), tenant.Scope{TenantID: "tenant-b", AppID: "app-a"}, ref)
	if err == nil {
		t.Fatal("secret resolved for another tenant scope")
	}

	resolver := sessionDSNResolver{defaultDSN: "postgres://metadata", secrets: provider}
	value, err = resolver.ResolveSessionDSN(context.Background(), storage.Handle{Scope: scope})
	if err != nil || value != "postgres://metadata" {
		t.Fatalf("resolve default session dsn = %q, %v", value, err)
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

type healthChecker func(context.Context) error

func (f healthChecker) Ping(ctx context.Context) error {
	return f(ctx)
}
