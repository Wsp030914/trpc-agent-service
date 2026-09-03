package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

const (
	defaultReplyPollInterval = time.Second
	defaultReplyLease        = 30 * time.Second
	defaultReplyBatch        = 32
	defaultReplyMaxAttempts  = 8
)

var (
	// ErrReplyBindingInactive means a binding was disabled after a reply was
	// claimed. The sender returns the row to PENDING so re-enabling the binding
	// can resume delivery without invoking Runner again.
	ErrReplyBindingInactive = errors.New("reply binding is inactive")
	// ErrReplyBindingChanged means a reply was created against an older
	// binding authorization snapshot and must not be sent with the new target.
	ErrReplyBindingChanged = errors.New("reply binding authorization changed")
)

// Build returns one durable text reply for a completed execution event.
// Events without user-visible assistant text return no replies while the
// execution event itself remains durable.
func BuildReplyEvent(
	ctx context.Context,
	exec Execution,
	sequence int64,
	evt *event.Event,
) ([]channels.Reply, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if sequence <= 0 {
		return nil, errors.New("reply event sequence must be positive")
	}
	if exec.RequestID == "" {
		return nil, errors.New("execution request_id is required")
	}
	if evt == nil {
		return nil, errors.New("runner event is required")
	}
	if evt.RequestID != "" && evt.RequestID != exec.RequestID {
		return nil, errors.New("runner event request_id does not match execution")
	}
	if exec.Tenant.Channel == "" || exec.Tenant.BindingID == "" || !evt.IsRunnerCompletion() {
		return nil, nil
	}
	if evt.Error != nil || evt.IsTerminalError() {
		return nil, nil
	}
	text := visibleAssistantText(evt)
	if text == "" {
		return nil, nil
	}
	target := channels.ReplyTarget{
		Kind:             channels.TargetKindUser,
		InternalEntityID: exec.Tenant.UserID,
	}
	if exec.Tenant.SessionPrincipalID != "" && exec.Tenant.SessionPrincipalID != exec.Tenant.UserID {
		target = channels.ReplyTarget{
			Kind:             channels.TargetKindConversation,
			InternalEntityID: exec.Tenant.SessionPrincipalID,
		}
	}
	reply := channels.Reply{
		TenantID:        exec.Tenant.TenantID,
		AppID:           exec.Tenant.AppID,
		RequestID:       exec.RequestID,
		SourceEventID:   fmt.Sprintf("%s:%d", exec.RequestID, sequence),
		Channel:         channels.Channel(exec.Tenant.Channel),
		BindingID:       exec.Tenant.BindingID,
		BindingRevision: exec.Tenant.BindingRevision,
		Revision:        sequence,
		Target:          target,
		Text:            text,
	}
	reply.ReplyID = reply.StableID()
	return []channels.Reply{reply}, nil
}

// ReplyOutbox persists and leases provider replies independently from the
// execution Dispatch Outbox.
type ReplyOutbox interface {
	ClaimReplies(context.Context, string, time.Duration, int) ([]ReplyDelivery, error)
	CompleteReply(context.Context, ReplyDelivery, channels.ProviderReceipt) error
	RetryReply(context.Context, ReplyDelivery, string, time.Duration, error) error
	FailReply(context.Context, ReplyDelivery, string, error) error
	RecoverReplyLeases(context.Context) error
}

// ReplyDelivery is one Reply Outbox row leased by a sender.
type ReplyDelivery struct {
	Reply             channels.Reply
	Attempt           int
	LeaseOwner        string
	LeaseUntil        time.Time
	ProviderMessageID string
	TraceID           string
	TraceParent       string
	TraceState        string
}

// Validate checks the identity and lease fields of a claimed reply.
func (d ReplyDelivery) Validate() error {
	if err := d.Reply.Validate(); err != nil {
		return fmt.Errorf("reply: %w", err)
	}
	if d.Attempt <= 0 || d.LeaseOwner == "" || d.LeaseUntil.IsZero() {
		return errors.New("reply delivery lease is incomplete")
	}
	return nil
}

// ReplyProvider is the provider client and optional rate limiter selected for
// one Reply Outbox delivery.
type ReplyProvider struct {
	Client  channels.ProviderOutboundClient
	Limiter ReplyRateLimiter
}

// ReplyRateLimiter delays or rejects one provider send within its bound
// provider account.
type ReplyRateLimiter interface {
	Allow(context.Context, ReplyDelivery) error
}

// ReplySenderOptions configures the finite Reply Outbox delivery loop.
type ReplySenderOptions struct {
	Owner        string
	Lease        time.Duration
	SendTimeout  time.Duration
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
	Metrics      *platformmetrics.Recorder
}

// ReplySender claims Reply Outbox rows, sends each row once, and records the
// bounded result. It never invokes Runner.
type ReplySender struct {
	outbox       ReplyOutbox
	targets      func(context.Context, ReplyDelivery) (string, error)
	providers    func(context.Context, ReplyDelivery) (ReplyProvider, error)
	owner        string
	lease        time.Duration
	sendTimeout  time.Duration
	pollInterval time.Duration
	batchSize    int
	maxAttempts  int
	metrics      *platformmetrics.Recorder
}

// NewReplySender creates a finite-retry Reply Outbox sender.
func NewReplySender(
	outbox ReplyOutbox,
	targets func(context.Context, ReplyDelivery) (string, error),
	providers func(context.Context, ReplyDelivery) (ReplyProvider, error),
	options ReplySenderOptions,
) (*ReplySender, error) {
	if outbox == nil || targets == nil || providers == nil {
		return nil, errors.New("reply sender dependencies are required")
	}
	if options.Owner == "" {
		return nil, errors.New("reply sender owner is required")
	}
	if options.Lease <= 0 {
		options.Lease = defaultReplyLease
	}
	if options.SendTimeout <= 0 {
		options.SendTimeout = options.Lease / 2
	}
	if options.SendTimeout <= 0 || options.SendTimeout >= options.Lease {
		return nil, errors.New("reply send timeout must be shorter than reply lease")
	}
	if options.PollInterval <= 0 {
		options.PollInterval = defaultReplyPollInterval
	}
	if options.BatchSize <= 0 {
		options.BatchSize = defaultReplyBatch
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = defaultReplyMaxAttempts
	}
	return &ReplySender{
		outbox:       outbox,
		targets:      targets,
		providers:    providers,
		owner:        options.Owner,
		lease:        options.Lease,
		sendTimeout:  options.SendTimeout,
		pollInterval: options.PollInterval,
		batchSize:    options.BatchSize,
		maxAttempts:  options.MaxAttempts,
		metrics:      options.Metrics,
	}, nil
}

// SendBatch claims and processes up to the configured batch size once.
func (s *ReplySender) SendBatch(ctx context.Context) (int, error) {
	if s == nil || s.outbox == nil || s.targets == nil || s.providers == nil {
		return 0, errors.New("reply sender is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := s.outbox.RecoverReplyLeases(ctx); err != nil {
		return 0, fmt.Errorf("recover reply leases: %w", err)
	}
	deliveries, err := s.outbox.ClaimReplies(ctx, s.owner, s.lease, s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("claim replies: %w", err)
	}
	for _, delivery := range deliveries {
		if err := s.sendOne(ctx, delivery); err != nil {
			return len(deliveries), err
		}
	}
	return len(deliveries), nil
}

// Run delivers replies until ctx is canceled.
func (s *ReplySender) Run(ctx context.Context) error {
	if s == nil {
		return errors.New("reply sender is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()
	for {
		if _, err := s.SendBatch(ctx); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *ReplySender) sendOne(ctx context.Context, delivery ReplyDelivery) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	parentCtx := platformtelemetry.Extract(ctx, map[string]string{
		"traceparent": delivery.TraceParent,
		"tracestate":  delivery.TraceState,
	})
	replyCtx, span := platformtelemetry.StartSpan(parentCtx, "reply.send",
		attribute.String("tenant_id", delivery.Reply.TenantID),
		attribute.String("app_id", delivery.Reply.AppID),
		attribute.String("channel", string(delivery.Reply.Channel)),
		attribute.String("request_id", delivery.Reply.RequestID),
	)
	defer span.End()
	started := time.Now()
	sendCtx, cancel := context.WithTimeout(replyCtx, s.sendTimeout)
	defer cancel()
	provider, err := s.providers(sendCtx, delivery)
	if err != nil {
		retryable, errorType := classifyReplyError(err)
		if errors.Is(err, ErrReplyBindingInactive) {
			retryable = true
			errorType = "binding_inactive"
		} else if errorType == "provider_permanent" {
			errorType = "provider_resolution"
		}
		return s.recordFailure(ctx, delivery, err, retryable, errorType, started, span)
	}
	if provider.Client == nil {
		return s.recordFailure(ctx, delivery, errors.New("reply provider client is not initialized"), false, "provider_client", started, span)
	}
	if provider.Limiter != nil {
		if err := provider.Limiter.Allow(sendCtx, delivery); err != nil {
			return s.recordFailure(ctx, delivery, err, true, "rate_limit", started, span)
		}
	}
	providerTarget, err := s.targets(sendCtx, delivery)
	if err != nil {
		return s.recordFailure(ctx, delivery, err, false, "target_resolution", started, span)
	}
	receipt, err := provider.Client.SendOnce(sendCtx, delivery.Reply, providerTarget)
	if err != nil {
		retryable, errorType := classifyReplyError(err)
		return s.recordFailure(ctx, delivery, err, retryable, errorType, started, span)
	}
	if err := receipt.Validate(); err != nil {
		return s.recordFailure(ctx, delivery, err, false, "invalid_provider_receipt", started, span)
	}
	if err := s.outbox.CompleteReply(ctx, delivery, receipt); err != nil {
		return fmt.Errorf("complete reply: %w", err)
	}
	if s.metrics != nil {
		s.metrics.RecordReply(ctx, platformmetrics.Labels{
			TenantID: delivery.Reply.TenantID,
			AppID:    delivery.Reply.AppID,
			Channel:  string(delivery.Reply.Channel),
		}, time.Since(started), "")
	}
	return nil
}

func (s *ReplySender) recordFailure(
	ctx context.Context,
	delivery ReplyDelivery,
	cause error,
	retryable bool,
	errorType string,
	started time.Time,
	span trace.Span,
) error {
	if errorType == "" {
		errorType = "provider_send"
	}
	platformtelemetry.MarkError(span, errorType, cause)
	if s.metrics != nil {
		s.metrics.RecordReply(ctx, platformmetrics.Labels{
			TenantID: delivery.Reply.TenantID,
			AppID:    delivery.Reply.AppID,
			Channel:  string(delivery.Reply.Channel),
		}, time.Since(started), errorType)
	}
	if retryable && delivery.Attempt < s.maxAttempts {
		delay := retryDelay(delivery.Attempt)
		if err := s.outbox.RetryReply(ctx, delivery, errorType, delay, cause); err != nil {
			return fmt.Errorf("retry reply: %w", err)
		}
		return nil
	}
	if err := s.outbox.FailReply(ctx, delivery, errorType, cause); err != nil {
		return fmt.Errorf("fail reply: %w", err)
	}
	return nil
}

func classifyReplyError(err error) (bool, string) {
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true, "transport_timeout"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true, "transport_timeout"
	}
	var retryableErr interface{ IsRetryable() bool }
	if errors.As(err, &retryableErr) && retryableErr.IsRetryable() {
		return true, "provider_retryable"
	}
	return false, "provider_permanent"
}

func retryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return time.Second
	}
	delay := time.Duration(attempt) * time.Second
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func visibleAssistantText(evt *event.Event) string {
	if evt == nil || evt.Response == nil {
		return ""
	}
	var text string
	for _, choice := range evt.Response.Choices {
		message := choice.Message
		if message.Role != "" && message.Role != model.RoleAssistant {
			continue
		}
		text += message.Content
	}
	return text
}
