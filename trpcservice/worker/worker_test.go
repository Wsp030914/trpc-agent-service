package worker_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestWorkerPrepareResolvesBackendWithoutStickySession(t *testing.T) {
	w := testWorker(t, sharedBackendProfile())

	exec, err := w.Prepare(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Backend.Name != "shared" {
		t.Fatalf("backend name = %q, want shared", exec.Backend.Name)
	}
	if exec.Config.Version != "v1" {
		t.Fatalf("config version = %q, want v1", exec.Config.Version)
	}
	if exec.TenantSource != gateway.TenantSourceAuthenticatedClaims {
		t.Fatalf("tenant source = %q, want authenticated claims", exec.TenantSource)
	}
	const want = "tenant:tenant-a:app:support:session:session-1"
	if exec.PartitionKey != want {
		t.Fatalf("partition key = %q, want %q", exec.PartitionKey, want)
	}
}

func TestWorkersPrepareSameJobWithoutNodeAffinity(t *testing.T) {
	backend := sharedBackendProfile()
	workers := []worker.Worker{
		testWorker(t, backend),
		testWorker(t, backend),
	}
	job := testJob("request-1", "tenant-a", "session-1")

	first, err := workers[0].Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("first worker prepare: %v", err)
	}
	second, err := workers[1].Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("second worker prepare: %v", err)
	}
	if first.PartitionKey != second.PartitionKey {
		t.Fatalf("partition keys differ: %q and %q", first.PartitionKey, second.PartitionKey)
	}
	if !reflect.DeepEqual(first.Backend, second.Backend) {
		t.Fatalf("backend profiles differ: %#v and %#v", first.Backend, second.Backend)
	}
	if !reflect.DeepEqual(first.Config, second.Config) {
		t.Fatalf("app configs differ: %#v and %#v", first.Config, second.Config)
	}
}

func TestWorkerResolvesExactConfigVersion(t *testing.T) {
	backend := sharedBackendProfile()
	v1 := testAppConfig("tenant-a", backend)
	v2 := testAppConfig("tenant-a", backend)
	v2.Version = "v2"
	v2.Model.Model = "gpt-4.1"
	configs, err := config.NewStaticResolver(v1, v2)
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	w := worker.Worker{
		Config:  configs,
		Storage: storage.StaticResolver{Backend: backend},
	}
	job := testJob("request-1", "tenant-a", "session-1")
	job.Tenant.ConfigVersion = "v2"

	exec, err := w.Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Config.Version != "v2" || exec.Config.Model.Model != "gpt-4.1" {
		t.Fatalf("resolved config = %q/%q, want v2/gpt-4.1", exec.Config.Version, exec.Config.Model.Model)
	}
}

func TestWorkerRequiresStorageResolver(t *testing.T) {
	cfg := testAppConfig("tenant-a", sharedBackendProfile())
	configs, err := config.NewStaticResolver(cfg)
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	if _, err := (worker.Worker{Config: configs}).Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded without storage resolver")
	}
}

func TestWorkerRequiresConfigResolver(t *testing.T) {
	w := worker.Worker{Storage: storage.StaticResolver{Backend: sharedBackendProfile()}}
	if _, err := w.Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded without config resolver")
	}
}

func TestWorkerRejectsBackendOutsideAppConfig(t *testing.T) {
	configured := sharedBackendProfile()
	resolved := sharedBackendProfile()
	configured.Session.Options = map[string]string{"schema": ""}
	resolved.Session.Options = map[string]string{"other": ""}
	configs, err := config.NewStaticResolver(testAppConfig("tenant-a", configured))
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	w := worker.Worker{
		Config:  configs,
		Storage: storage.StaticResolver{Backend: resolved},
	}

	if _, err := w.Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded with backend outside app config")
	}
}

func testJob(requestID, tenantID, sessionID string) gateway.Job {
	return gateway.Job{
		RequestID:    requestID,
		TenantSource: gateway.TenantSourceAuthenticatedClaims,
		Tenant: tenant.RuntimeContext{
			TenantID:           tenantID,
			AppID:              "support",
			ConfigVersion:      "v1",
			SessionID:          sessionID,
			SessionPrincipalID: "principal-1",
		},
		Message: gateway.Message{Text: "hello"},
	}
}

func sharedBackendProfile() tenant.BackendProfile {
	return tenant.BackendProfile{
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

func testWorker(t *testing.T, backend tenant.BackendProfile) worker.Worker {
	t.Helper()
	configs, err := config.NewStaticResolver(testAppConfig("tenant-a", backend))
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	return worker.Worker{
		Config:  configs,
		Storage: storage.StaticResolver{Backend: backend},
	}
}

func testAppConfig(tenantID string, backend tenant.BackendProfile) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider: "openai",
			Model:    "gpt-4.1-mini",
		},
		Backend: backend,
	}
}
