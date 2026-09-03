package gateway_test

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestQueuedRunnerAdmitsAuthenticatedContextAndForwardsPersistedEvents(t *testing.T) {
	identity := validAdmissionIdentity()
	resolver := staticTenantResolver{
		tenant:       identity.Tenant,
		source:       identity.Source,
		identity:     identity,
		withIdentity: true,
	}
	ctx, err := gateway.ContextWithAuthenticatedRequest(context.Background(), gateway.AuthenticatedRequest{
		RequestID:      "request-queued-1",
		IdempotencyKey: "idempotency-queued-1",
		Tenant:         resolver,
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID:     "request-queued-1",
		ConfigVersion: "v1",
		TurnSeq:       1,
	}}
	source := &staticExecutionEventSource{events: []gateway.ExecutionEvent{{
		Sequence: 1,
		Event: event.NewResponseEvent("invocation-1", "assistant", &model.Response{
			ID:     "event-1",
			Object: model.ObjectTypeRunnerCompletion,
			Done:   true,
		}),
	}}}
	queued, err := gateway.NewQueuedRunner(gateway.New(admitter), source)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}

	events, err := queued.Run(ctx, "untrusted-user", "untrusted-session", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatalf("run queued request: %v", err)
	}
	got, ok := <-events
	if !ok {
		t.Fatal("queued runner closed without persisted event")
	}
	if got.Response == nil || got.Response.ID != "event-1" {
		t.Fatalf("forwarded event = %#v", got)
	}
	if _, ok := <-events; ok {
		t.Fatal("queued runner did not close after persisted stream")
	}
	if admitter.request.RequestID != "request-queued-1" ||
		admitter.request.IdempotencyKey != "idempotency-queued-1" {
		t.Fatalf("admitted request = %#v", admitter.request)
	}
	if admitter.request.Identity.Tenant != identity.Tenant {
		t.Fatalf("admitted identity tenant = %#v, want %#v", admitter.request.Identity.Tenant, identity.Tenant)
	}
	if source.scope != identity.Tenant.Scope() || source.requestID != "request-queued-1" || source.after != 0 {
		t.Fatalf("event subscription = %#v", source)
	}
}

func TestQueuedRunnerRejectsRuntimeOptions(t *testing.T) {
	identity := validAdmissionIdentity()
	ctx, err := gateway.ContextWithAuthenticatedRequest(context.Background(), gateway.AuthenticatedRequest{
		RequestID:      "request-queued-2",
		IdempotencyKey: "idempotency-queued-2",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
	})
	if err != nil {
		t.Fatalf("attach authenticated request: %v", err)
	}
	admitter := &captureAdmitter{result: gateway.AdmissionResult{
		RequestID:     "request-queued-2",
		ConfigVersion: "v1",
		TurnSeq:       1,
	}}
	queued, err := gateway.NewQueuedRunner(
		gateway.New(admitter),
		&staticExecutionEventSource{},
	)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}

	_, err = queued.Run(
		ctx,
		"user-1",
		"session-1",
		model.NewUserMessage("hello"),
		agent.MergeRuntimeState(map[string]any{"tenant_id": "other"}),
	)
	if err == nil || !strings.Contains(err.Error(), "runner options are not supported") {
		t.Fatalf("queued runner error = %v", err)
	}
	if admitter.request.RequestID != "" {
		t.Fatal("queued runner admitted a request with runtime options")
	}
}

type staticExecutionEventSource struct {
	events    []gateway.ExecutionEvent
	scope     tenant.Scope
	requestID string
	after     int64
}

func (s *staticExecutionEventSource) SubscribeExecutionEvents(
	_ context.Context,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) (<-chan gateway.ExecutionEvent, error) {
	s.scope = scope
	s.requestID = requestID
	s.after = afterSequence
	stream := make(chan gateway.ExecutionEvent, len(s.events))
	for _, item := range s.events {
		stream <- item
	}
	close(stream)
	return stream, nil
}
