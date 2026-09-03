package worker_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestConsumerAcknowledgesAfterExecutionCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-success")
	stream := &testStream{delivery: queue.Delivery{ID: "1-0", Dispatch: queue.Dispatch{OutboxID: 1, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.completed != queue.CompletionSucceeded {
		t.Fatalf("completion = %q", store.completed)
	}
	if stream.acks != 1 {
		t.Fatalf("acks = %d", stream.acks)
	}
}

func TestConsumerRetriesAndAcknowledgesFailedRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-retry")
	stream := &testStream{delivery: queue.Delivery{ID: "2-0", Dispatch: queue.Dispatch{OutboxID: 2, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{err: errors.New("runner failed")}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.retries != 1 || stream.acks != 1 {
		t.Fatalf("retries=%d acks=%d", store.retries, stream.acks)
	}
}

func TestConsumerExtractsTraceContextFromDispatch(t *testing.T) {
	runtime := platformtelemetry.NewNoop(context.Background(), "consumer-test")
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	parentCtx, parentSpan := platformtelemetry.StartSpan(context.Background(), "gateway")
	defer parentSpan.End()
	wantTraceID := platformtelemetry.TraceID(parentCtx)
	carrier := platformtelemetry.Inject(parentCtx)
	if wantTraceID == "" || carrier["traceparent"] == "" {
		t.Fatal("parent trace context was not created")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-trace")
	stream := &testStream{delivery: queue.Delivery{
		ID: "3-0",
		Dispatch: queue.Dispatch{
			OutboxID:    3,
			TenantID:    "tenant-a",
			AppID:       "support",
			RequestID:   claim.Job.RequestID(),
			TraceParent: carrier["traceparent"],
			TraceState:  carrier["tracestate"],
		},
	}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	executor := &consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}
	consumer, err := worker.NewConsumer(executor, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if executor.traceID != wantTraceID {
		t.Fatalf("executor trace id = %q, want %q", executor.traceID, wantTraceID)
	}
}

type consumerExecutor struct {
	result  worker.RunResult
	err     error
	traceID string
}

func (e *consumerExecutor) Run(ctx context.Context, _ execution.Job) (worker.RunResult, error) {
	if _, ok := worker.JobLeaseFromContext(ctx); !ok {
		return worker.RunResult{}, errors.New("lease is missing")
	}
	e.traceID = platformtelemetry.TraceID(ctx)
	return e.result, e.err
}

type testStream struct {
	mu        sync.Mutex
	delivery  queue.Delivery
	delivered bool
	acks      int
	cancel    context.CancelFunc
}

func (s *testStream) Receive(ctx context.Context, _ string, _ time.Duration) (queue.Delivery, error) {
	s.mu.Lock()
	if !s.delivered {
		s.delivered = true
		s.mu.Unlock()
		return s.delivery, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return queue.Delivery{}, ctx.Err()
}
func (s *testStream) Ack(_ context.Context, _ queue.Delivery) error {
	s.mu.Lock()
	s.acks++
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	return nil
}
func (s *testStream) Dead(context.Context, queue.Delivery, error) error { return nil }

type testExecutionStore struct {
	claim     queue.Claim
	claimed   bool
	completed queue.CompletionStatus
	retries   int
}

func (s *testExecutionStore) Claim(_ context.Context, _ queue.Dispatch, _ queue.ClaimRequest) (queue.Claim, bool, error) {
	if s.claimed {
		return queue.Claim{}, false, nil
	}
	s.claimed = true
	return s.claim, true, nil
}
func (s *testExecutionStore) Renew(_ context.Context, c queue.Claim, _ time.Duration) (queue.Lease, error) {
	return c.Lease, nil
}
func (s *testExecutionStore) Complete(_ context.Context, _ queue.Claim, status queue.CompletionStatus) error {
	s.completed = status
	return nil
}
func (s *testExecutionStore) Retry(_ context.Context, _ queue.Claim, _ error) error {
	s.retries++
	return nil
}
func testQueueClaim(t *testing.T, requestID string) queue.Claim {
	t.Helper()
	return queue.Claim{Job: testJob(requestID, "tenant-a", "session-consumer"), TurnSeq: 1, Lease: queue.Lease{Owner: "worker-1", Token: "token-1", Until: time.Now().Add(time.Minute)}, Attempt: 1}
}
