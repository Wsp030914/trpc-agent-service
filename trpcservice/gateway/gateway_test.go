package gateway_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type captureQueue struct {
	job gateway.Job
}

func (q *captureQueue) Enqueue(_ context.Context, job gateway.Job) error {
	q.job = job
	return nil
}

func TestGatewayCreatesTenantScopedJob(t *testing.T) {
	queue := &captureQueue{}
	gw := gateway.Gateway{Jobs: queue}

	req := gateway.Request{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "support",
			ConfigVersion:      "v1",
			SessionID:          "session-1",
			SessionPrincipalID: "principal-1",
			ActorUserID:        "actor-1",
		},
		Message: gateway.Message{Text: "hello"},
	}

	job, err := gw.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if queue.job.RequestID != req.RequestID {
		t.Fatalf("queued request ID = %q, want %q", queue.job.RequestID, req.RequestID)
	}
	if job.Tenant.TenantID != "tenant-a" {
		t.Fatalf("job tenant ID = %q, want tenant-a", job.Tenant.TenantID)
	}
	key, err := job.PartitionKey()
	if err != nil {
		t.Fatalf("partition key: %v", err)
	}
	const want = "tenant:tenant-a:app:support:session:session-1"
	if key != want {
		t.Fatalf("partition key = %q, want %q", key, want)
	}
}
