package queue_test

import (
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestClaimRequestValidate(t *testing.T) {
	request := queue.ClaimRequest{Owner: "worker-1", LeaseDuration: time.Second}
	if err := request.Validate(); err != nil {
		t.Fatalf("validate claim request: %v", err)
	}

	request.Owner = ""
	if err := request.Validate(); err == nil {
		t.Fatal("validate claim request succeeded without owner")
	}
}

func TestClaimValidate(t *testing.T) {
	job, err := execution.NewJob(
		"request-1",
		gateway.TenantSourceAuthenticatedClaims,
		tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "support",
			ConfigVersion:      "v1",
			SessionID:          "session-1",
			SessionPrincipalID: "user-1",
			UserID:             "user-1",
			TraceID:            "trace-1",
		},
		gateway.Message{Text: "hello"},
	)
	if err != nil {
		t.Fatalf("new execution job: %v", err)
	}
	claim := queue.Claim{
		Job:     job,
		TurnSeq: 1,
		Lease: queue.Lease{
			Owner: "worker-1",
			Token: "run-token-1",
			Until: time.Now().Add(time.Second),
		},
		Attempt: 1,
	}
	if err := claim.Validate(); err != nil {
		t.Fatalf("validate claim: %v", err)
	}

	claim.Attempt = 0
	if err := claim.Validate(); err == nil {
		t.Fatal("validate claim succeeded without attempt")
	}
}

func TestCompletionStatusValidate(t *testing.T) {
	for _, status := range []queue.CompletionStatus{
		queue.CompletionSucceeded,
		queue.CompletionFailed,
	} {
		if err := status.Validate(); err != nil {
			t.Fatalf("validate completion status %q: %v", status, err)
		}
	}
	if err := queue.CompletionStatus("PENDING").Validate(); err == nil {
		t.Fatal("validate completion status succeeded for pending")
	}
}
