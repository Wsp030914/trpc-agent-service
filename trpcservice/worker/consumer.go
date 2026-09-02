package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

const (
	defaultConsumerLeaseDuration = 30 * time.Second
	defaultConsumerPollInterval  = time.Second
	defaultConsumerConcurrency   = 4
	consumerCompletionTimeout    = 5 * time.Second
)

// JobExecutor executes one durable execution claimed by a Consumer.
type JobExecutor interface {
	Run(context.Context, execution.Job) (RunResult, error)
}

// ExecutionStore owns PostgreSQL execution transitions. Redis delivery is not
// authoritative: every transition is conditioned on the run token.
type ExecutionStore interface {
	Claim(context.Context, queue.Dispatch, queue.ClaimRequest) (queue.Claim, bool, error)
	Renew(context.Context, queue.Claim, time.Duration) (queue.Lease, error)
	Complete(context.Context, queue.Claim, queue.CompletionStatus) error
	Retry(context.Context, queue.Claim, error) error
}

// ConsumerOption configures a Consumer.
type ConsumerOption func(*Consumer)

// WithConsumerLeaseDuration sets the PostgreSQL execution lease duration.
func WithConsumerLeaseDuration(duration time.Duration) ConsumerOption {
	return func(c *Consumer) { c.leaseDuration = duration }
}

// WithConsumerPollInterval sets the maximum Redis Stream receive wait.
func WithConsumerPollInterval(interval time.Duration) ConsumerOption {
	return func(c *Consumer) { c.pollInterval = interval }
}

// WithConsumerConcurrency caps how many claimed jobs one consumer executes at
// once. Session-lane ordering is preserved because the execution store defers
// a turn until earlier turns of the same session reach a terminal state.
func WithConsumerConcurrency(concurrency int) ConsumerOption {
	return func(c *Consumer) { c.concurrency = concurrency }
}

// Consumer reads Redis Stream entries, claims their PostgreSQL execution, and
// acknowledges Redis only after the persistent transition is complete.
type Consumer struct {
	executor      JobExecutor
	stream        queue.Stream
	jobs          ExecutionStore
	owner         string
	leaseDuration time.Duration
	pollInterval  time.Duration
	concurrency   int
	mu            sync.Mutex
	stopClaims    chan struct{}
	claimCancel   context.CancelFunc
}

// NewConsumer creates a consumer for one stable worker identity.
func NewConsumer(executor JobExecutor, stream queue.Stream, jobs ExecutionStore, owner string, opts ...ConsumerOption) (*Consumer, error) {
	if executor == nil {
		return nil, errors.New("job executor is required")
	}
	if stream == nil {
		return nil, errors.New("dispatch stream is required")
	}
	if jobs == nil {
		return nil, errors.New("execution store is required")
	}
	if owner == "" {
		return nil, errors.New("worker owner is required")
	}
	c := &Consumer{executor: executor, stream: stream, jobs: jobs, owner: owner, leaseDuration: defaultConsumerLeaseDuration, pollInterval: defaultConsumerPollInterval, concurrency: defaultConsumerConcurrency, stopClaims: make(chan struct{})}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	if c.leaseDuration <= 0 {
		return nil, errors.New("consumer lease duration must be positive")
	}
	if c.pollInterval <= 0 {
		return nil, errors.New("consumer poll interval must be positive")
	}
	if c.concurrency <= 0 {
		return nil, errors.New("consumer concurrency must be positive")
	}
	return c, nil
}

// StopClaiming stops reads while allowing a previously claimed run to drain.
func (c *Consumer) StopClaiming() {
	if c == nil {
		return
	}
	c.mu.Lock()
	select {
	case <-c.stopClaims:
		c.mu.Unlock()
		return
	default:
		close(c.stopClaims)
		cancel := c.claimCancel
		c.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}
}

// Run consumes deliveries until cancellation, an unrecoverable stream error,
// or StopClaiming. Claimed jobs execute with bounded concurrency; the
// execution store preserves session-lane order across concurrent runs. Pending
// Redis deliveries are reclaimed after an owner dies.
func (c *Consumer) Run(ctx context.Context) error {
	if c == nil || c.executor == nil || c.stream == nil || c.jobs == nil {
		return errors.New("consumer is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	claimCtx, cancel, stopped := c.claimContext(ctx)
	if stopped {
		return nil
	}
	defer c.clearClaimContext(cancel)

	sem := make(chan struct{}, c.concurrency)
	var wg sync.WaitGroup
	// Wait for in-flight executions before returning so their lease renewal
	// and PostgreSQL transitions are not abandoned mid-flight.
	defer wg.Wait()

	var failOnce sync.Once
	var runErr error
	fail := func(err error) {
		failOnce.Do(func() {
			runErr = err
			c.StopClaiming()
		})
	}

	for {
		if c.claimingStopped() {
			return runErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		delivery, err := c.stream.Receive(claimCtx, c.owner, c.pollInterval)
		if errors.Is(err, context.DeadlineExceeded) {
			continue
		}
		if err != nil {
			if c.claimingStopped() && errors.Is(err, context.Canceled) {
				return runErr
			}
			return fmt.Errorf("receive dispatch: %w", err)
		}
		if err := delivery.Validate(); err != nil {
			if deadErr := c.stream.Dead(context.WithoutCancel(ctx), delivery, err); deadErr != nil {
				return fmt.Errorf("dead-letter invalid dispatch: %w", deadErr)
			}
			continue
		}
		claim, found, err := c.jobs.Claim(claimCtx, delivery.Dispatch, queue.ClaimRequest{Owner: c.owner, LeaseDuration: c.leaseDuration})
		if err != nil {
			if c.claimingStopped() && errors.Is(err, context.Canceled) {
				return runErr
			}
			return fmt.Errorf("claim execution: %w", err)
		}
		if !found {
			if err := c.stream.Ack(context.WithoutCancel(ctx), delivery); err != nil {
				return fmt.Errorf("ack unavailable execution: %w", err)
			}
			continue
		}
		if err := claim.Validate(); err != nil {
			return fmt.Errorf("claimed execution: %w", err)
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		wg.Add(1)
		go func(claim queue.Claim, delivery queue.Delivery) {
			defer wg.Done()
			defer func() { <-sem }()
			ack, err := c.executeClaim(ctx, claim)
			if err != nil {
				fail(err)
				return
			}
			if ack {
				if err := c.stream.Ack(context.WithoutCancel(ctx), delivery); err != nil {
					fail(fmt.Errorf("ack execution: %w", err))
				}
			}
		}(claim, delivery)
	}
}
func (c *Consumer) claimContext(parent context.Context) (context.Context, context.CancelFunc, bool) {
	ctx, cancel := context.WithCancel(parent)
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stopClaims:
		cancel()
		return ctx, cancel, true
	default:
		c.claimCancel = cancel
		return ctx, cancel, false
	}
}
func (c *Consumer) clearClaimContext(cancel context.CancelFunc) {
	c.mu.Lock()
	c.claimCancel = nil
	c.mu.Unlock()
	cancel()
}
func (c *Consumer) claimingStopped() bool {
	select {
	case <-c.stopClaims:
		return true
	default:
		return false
	}
}
func (c *Consumer) executeClaim(ctx context.Context, claim queue.Claim) (bool, error) {
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var err error
	runCtx, err = ContextWithJobLease(runCtx, claim.Lease)
	if err != nil {
		return false, fmt.Errorf("attach execution lease: %w", err)
	}
	done := make(chan error, 1)
	go c.renewLease(runCtx, cancelRun, claim, done)
	result, runErr := c.executor.Run(runCtx, claim.Job)
	cancelRun()
	leaseErr := <-done
	if errors.Is(leaseErr, queue.ErrLeaseLost) {
		return true, nil
	}
	if leaseErr != nil {
		return false, fmt.Errorf("renew execution lease: %w", leaseErr)
	}
	completeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consumerCompletionTimeout)
	defer cancel()
	if ctx.Err() != nil {
		return true, nil
	}
	if runErr != nil || !result.RunnerCompleted {
		failure := runErr
		if failure == nil {
			failure = errors.New("runner completed without completion event")
		}
		if err := c.jobs.Retry(completeCtx, claim, failure); errors.Is(err, queue.ErrLeaseLost) {
			return true, nil
		} else if err != nil {
			return false, fmt.Errorf("retry execution: %w", err)
		}
		return true, nil
	}
	if err := c.jobs.Complete(completeCtx, claim, queue.CompletionSucceeded); errors.Is(err, queue.ErrLeaseLost) {
		return true, nil
	} else if err != nil {
		return false, fmt.Errorf("complete execution: %w", err)
	}
	return true, nil
}
func (c *Consumer) renewLease(ctx context.Context, cancel context.CancelFunc, claim queue.Claim, done chan<- error) {
	defer close(done)
	interval := c.leaseDuration / 2
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lease, err := c.jobs.Renew(ctx, claim, c.leaseDuration)
			if err != nil {
				if ctx.Err() == nil {
					done <- err
					cancel()
				}
				return
			}
			claim.Lease = lease
		}
	}
}

var _ JobExecutor = (*Worker)(nil)
