package worker_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestWorkerPrepareResolvesBackendWithoutStickySession(t *testing.T) {
	w := worker.Worker{
		Storage: storage.StaticResolver{
			Backend: tenant.BackendProfile{Name: "sql-default"},
		},
	}

	exec, err := w.Prepare(context.Background(), gateway.Job{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "support",
			ConfigVersion:      "v1",
			SessionID:          "session-1",
			SessionPrincipalID: "principal-1",
		},
		Message: gateway.Message{Text: "hello"},
	})
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Backend.Name != "sql-default" {
		t.Fatalf("backend name = %q, want sql-default", exec.Backend.Name)
	}
	const want = "tenant:tenant-a:app:support:session:session-1"
	if exec.PartitionKey != want {
		t.Fatalf("partition key = %q, want %q", exec.PartitionKey, want)
	}
}
