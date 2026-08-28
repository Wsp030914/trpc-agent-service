package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	goredis "github.com/redis/go-redis/v9"
)

const dispatchPayloadField = "dispatch"

// Stream is one Redis Stream Consumer Group used for worker dispatches.
type Stream struct {
	client       *goredis.Client
	name         string
	group        string
	claimMinIdle time.Duration
}

// NewStream creates a stream Consumer Group. Existing groups are reused.
func NewStream(client *Client, name, group string, claimMinIdle time.Duration) (*Stream, error) {
	if client == nil || client.client == nil {
		return nil, errors.New("redis client is required")
	}
	if name == "" || group == "" {
		return nil, errors.New("redis stream name and group are required")
	}
	if claimMinIdle <= 0 {
		return nil, errors.New("redis pending claim idle duration must be positive")
	}
	return &Stream{
		client:       client.client,
		name:         name,
		group:        group,
		claimMinIdle: claimMinIdle,
	}, nil
}

// Init creates the Consumer Group and stream when they do not exist.
func (s *Stream) Init(ctx context.Context) error {
	if s == nil || s.client == nil {
		return errors.New("redis stream is not initialized")
	}
	err := s.client.XGroupCreateMkStream(ctx, s.name, s.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create redis stream consumer group: %w", err)
	}
	return nil
}

// Publish appends a durable dispatch to the worker stream.
func (s *Stream) Publish(ctx context.Context, dispatch queue.Dispatch) error {
	if s == nil || s.client == nil {
		return errors.New("redis stream is not initialized")
	}
	if err := dispatch.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(dispatch)
	if err != nil {
		return fmt.Errorf("marshal dispatch: %w", err)
	}
	if err := s.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: s.name,
		Values: map[string]any{dispatchPayloadField: payload},
	}).Err(); err != nil {
		return fmt.Errorf("publish redis dispatch: %w", err)
	}
	return nil
}

// Receive claims one abandoned pending message before reading a new delivery.
func (s *Stream) Receive(ctx context.Context, consumer string, block time.Duration) (queue.Delivery, error) {
	if s == nil || s.client == nil {
		return queue.Delivery{}, errors.New("redis stream is not initialized")
	}
	if consumer == "" {
		return queue.Delivery{}, errors.New("redis consumer is required")
	}
	if block <= 0 {
		return queue.Delivery{}, errors.New("redis receive block duration must be positive")
	}
	claimed, _, err := s.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   s.name,
		Group:    s.group,
		Consumer: consumer,
		MinIdle:  s.claimMinIdle,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return queue.Delivery{}, fmt.Errorf("claim pending redis dispatch: %w", err)
	}
	if len(claimed) > 0 {
		return decodeDelivery(claimed[0])
	}
	result, err := s.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    s.group,
		Consumer: consumer,
		Streams:  []string{s.name, ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, goredis.Nil) {
		return queue.Delivery{}, context.DeadlineExceeded
	}
	if err != nil {
		return queue.Delivery{}, fmt.Errorf("read redis dispatch: %w", err)
	}
	if len(result) == 0 || len(result[0].Messages) == 0 {
		return queue.Delivery{}, context.DeadlineExceeded
	}
	return decodeDelivery(result[0].Messages[0])
}

// Ack acknowledges one delivery after its PostgreSQL execution transition.
func (s *Stream) Ack(ctx context.Context, delivery queue.Delivery) error {
	if err := delivery.Validate(); err != nil {
		return err
	}
	if err := s.client.XAck(ctx, s.name, s.group, delivery.ID).Err(); err != nil {
		return fmt.Errorf("ack redis dispatch: %w", err)
	}
	return nil
}

// Dead appends an undecodable delivery to a stream-local DLQ and acknowledges it.
func (s *Stream) Dead(ctx context.Context, delivery queue.Delivery, cause error) error {
	if s == nil || s.client == nil {
		return errors.New("redis stream is not initialized")
	}
	if delivery.ID == "" {
		return errors.New("stream delivery id is required")
	}
	if err := s.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: s.name + ":dlq",
		Values: map[string]any{
			"source_id": delivery.ID,
			"error":     truncateError(cause),
		},
	}).Err(); err != nil {
		return fmt.Errorf("write redis dispatch dlq: %w", err)
	}
	if err := s.client.XAck(ctx, s.name, s.group, delivery.ID).Err(); err != nil {
		return fmt.Errorf("ack redis dead-letter delivery: %w", err)
	}
	return nil
}

func decodeDelivery(message goredis.XMessage) (queue.Delivery, error) {
	raw, ok := message.Values[dispatchPayloadField]
	if !ok {
		return queue.Delivery{}, errors.New("redis dispatch payload is missing")
	}
	var bytes []byte
	switch value := raw.(type) {
	case string:
		bytes = []byte(value)
	case []byte:
		bytes = value
	default:
		return queue.Delivery{}, fmt.Errorf("redis dispatch payload type %T is unsupported", raw)
	}
	var dispatch queue.Dispatch
	if err := json.Unmarshal(bytes, &dispatch); err != nil {
		return queue.Delivery{}, fmt.Errorf("decode redis dispatch: %w", err)
	}
	delivery := queue.Delivery{ID: message.ID, Dispatch: dispatch}
	if err := delivery.Validate(); err != nil {
		return queue.Delivery{}, err
	}
	return delivery, nil
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 512 {
		return value[:512]
	}
	return value
}

var _ queue.Publisher = (*Stream)(nil)
var _ queue.Stream = (*Stream)(nil)
