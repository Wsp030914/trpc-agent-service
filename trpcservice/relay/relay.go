// Package relay publishes committed execution outbox rows to worker streams.
package relay

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

const (
	defaultLease = 30 * time.Second
	defaultPoll  = time.Second
)

// Store leases committed outbox rows and records relay outcomes.
type Store interface {
	ClaimDispatches(context.Context, string, time.Duration, int) ([]queue.Dispatch, error)
	CompleteDispatch(context.Context, queue.Dispatch, string) error
	RetryDispatch(context.Context, queue.Dispatch, string, error) error
	RecoverDispatches(context.Context, time.Duration) error
}

// Relay publishes PostgreSQL outbox rows to the shared worker stream. Duplicate
// stream entries are safe because workers condition claims on execution state.
type Relay struct {
	store     Store
	publisher queue.Publisher
	owner     string
	lease     time.Duration
	poll      time.Duration
}

// New creates a transactional-outbox relay.
func New(store Store, publisher queue.Publisher, owner string) (*Relay, error) {
	if store == nil {
		return nil, errors.New("dispatch store is required")
	}
	if publisher == nil {
		return nil, errors.New("dispatch publisher is required")
	}
	if owner == "" {
		return nil, errors.New("dispatcher owner is required")
	}
	return &Relay{store: store, publisher: publisher, owner: owner, lease: defaultLease, poll: defaultPoll}, nil
}

// Run continuously recovers stale rows and publishes ready outbox entries.
func (r *Relay) Run(ctx context.Context) error {
	if r == nil || r.store == nil || r.publisher == nil {
		return errors.New("relay is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := r.store.RecoverDispatches(ctx, r.lease); err != nil {
			return fmt.Errorf("recover dispatches: %w", err)
		}
		items, err := r.store.ClaimDispatches(ctx, r.owner, r.lease, 0)
		if err != nil {
			return fmt.Errorf("claim dispatches: %w", err)
		}
		for _, item := range items {
			if err := r.publisher.Publish(ctx, item); err != nil {
				if retryErr := r.store.RetryDispatch(context.WithoutCancel(ctx), item, r.owner, err); retryErr != nil {
					return fmt.Errorf("retry dispatch: %w", retryErr)
				}
				continue
			}
			if err := r.store.CompleteDispatch(ctx, item, r.owner); err != nil {
				return fmt.Errorf("complete dispatch: %w", err)
			}
		}
		if len(items) != 0 {
			continue
		}
		timer := time.NewTimer(r.poll)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
