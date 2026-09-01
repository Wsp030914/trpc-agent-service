package worker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
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

// ReplyCapabilityResolver supplies the provider capability for one trusted
// execution. It must resolve by tenant, application, and binding scope.
type ReplyCapabilityResolver interface {
	ResolveReplyCapability(context.Context, Execution) (channels.ProviderCapability, error)
}

// ReplyCapabilityResolverFunc adapts a function to ReplyCapabilityResolver.
type ReplyCapabilityResolverFunc func(context.Context, Execution) (channels.ProviderCapability, error)

// ResolveReplyCapability implements ReplyCapabilityResolver.
func (f ReplyCapabilityResolverFunc) ResolveReplyCapability(ctx context.Context, exec Execution) (channels.ProviderCapability, error) {
	if f == nil {
		return channels.ProviderCapability{}, errors.New("reply capability resolver is not initialized")
	}
	return f(ctx, exec)
}

// ReplyEventBuilder converts user-visible Runner output into platform Reply
// values. It has no persistence or provider lifecycle responsibility.
type ReplyEventBuilder struct {
	capabilities ReplyCapabilityResolver
}

// ReplyEventBuilderSource is the small contract consumed by the durable event
// journal. Implementations only build values; the journal owns persistence.
type ReplyEventBuilderSource interface {
	Build(context.Context, Execution, int64, *event.Event) ([]channels.Reply, error)
}

// NewReplyEventBuilder creates a Runner-event to Reply builder.
func NewReplyEventBuilder(resolver ReplyCapabilityResolver) (*ReplyEventBuilder, error) {
	if resolver == nil {
		return nil, errors.New("reply capability resolver is required")
	}
	return &ReplyEventBuilder{capabilities: resolver}, nil
}

var _ ReplyEventBuilderSource = (*ReplyEventBuilder)(nil)

// Build returns platform replies for one persisted execution event. Events
// without user-visible assistant text return no replies and are still expected
// to advance the durable projection cursor in the persistence layer.
func (b *ReplyEventBuilder) Build(
	ctx context.Context,
	exec Execution,
	sequence int64,
	evt *event.Event,
) ([]channels.Reply, error) {
	if b == nil || b.capabilities == nil {
		return nil, errors.New("reply event builder is not initialized")
	}
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
	if exec.Tenant.Channel == "" || exec.Tenant.BindingID == "" {
		return nil, nil
	}
	capability, err := b.capabilities.ResolveReplyCapability(ctx, exec)
	if err != nil {
		return nil, fmt.Errorf("resolve reply capability: %w", err)
	}
	if err := capability.Validate(); err != nil {
		return nil, fmt.Errorf("reply capability: %w", err)
	}
	if evt.Error != nil || evt.IsTerminalError() {
		return nil, nil
	}
	text, contentDelta := visibleAssistantText(evt)
	if text == "" {
		return nil, nil
	}
	completion := evt.IsRunnerCompletion()
	if !capability.SupportsStreaming && !completion {
		return nil, nil
	}
	var parts []string
	if contentDelta {
		if len([]byte(text)) > capability.MaxTextSize {
			return nil, errors.New("reply delta exceeds provider text limit")
		}
		parts = []string{text}
	} else {
		parts, err = splitReplyText(text, capability.MaxTextSize)
		if err != nil {
			return nil, err
		}
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
	sourceEventID := fmt.Sprintf("%s:%d", exec.RequestID, sequence)
	logicalReplyID := exec.RequestID + ":assistant"
	operation := channels.ReplyOperationSend
	if completion && capability.SupportsStreaming {
		if capability.SupportsFinalize {
			operation = channels.ReplyOperationFinalize
		} else if capability.SupportsUpdate {
			operation = channels.ReplyOperationUpdate
		}
	}
	replies := make([]channels.Reply, 0, len(parts))
	for index, part := range parts {
		reply := channels.Reply{
			TenantID:        exec.Tenant.TenantID,
			AppID:           exec.Tenant.AppID,
			RequestID:       exec.RequestID,
			SourceEventID:   sourceEventID,
			Channel:         channels.Channel(exec.Tenant.Channel),
			BindingID:       exec.Tenant.BindingID,
			BindingRevision: exec.Tenant.BindingRevision,
			LogicalReplyID:  logicalReplyID,
			PartNo:          int64(index + 1),
			Revision:        sequence,
			Operation:       operation,
			Kind:            channels.ReplyKindText,
			Target:          target,
			Text:            part,
			ContentDelta:    contentDelta,
		}
		reply.ReplyID = stableReplyID(reply)
		replies = append(replies, reply)
	}
	return replies, nil
}

// ReplyOutbox persists and leases provider replies independently from the
// execution Dispatch Outbox.
type ReplyOutbox interface {
	PutReply(context.Context, channels.Reply) error
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

// ReplyProvider is the one-call provider capability and client selected for a
// Reply Outbox delivery.
type ReplyProvider struct {
	Capability channels.ProviderCapability
	Client     channels.ProviderOutboundClient
	Classifier ReplyErrorClassifier
	Limiter    ReplyRateLimiter
}

// ReplyProviderResolver selects a provider client only after the delivery's
// tenant, application, binding, and channel scope is known.
type ReplyProviderResolver interface {
	ResolveReplyProvider(context.Context, ReplyDelivery) (ReplyProvider, error)
}

// ReplyTargetResolver decrypts a target envelope immediately before one
// provider call. Implementations must never return or log the target earlier.
type ReplyTargetResolver interface {
	ResolveReplyTarget(context.Context, ReplyDelivery) (string, channels.OutboundContext, error)
}

// ReplyRateLimiter delays or rejects one provider operation within its bound
// provider account. A limiter error is classified by ReplySender.
type ReplyRateLimiter interface {
	Allow(context.Context, ReplyDelivery) error
}

// ReplyErrorClassifier maps a provider error to the finite Reply Outbox
// retry policy. errorType must not contain response bodies or credentials.
type ReplyErrorClassifier interface {
	Classify(error) (retryable bool, errorType string)
}

// ReplySenderOptions configures the finite Reply Outbox delivery loop.
type ReplySenderOptions struct {
	Owner        string
	Lease        time.Duration
	SendTimeout  time.Duration
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
}

// ReplySender claims Reply Outbox rows, sends each row once, and records the
// bounded result. It never invokes Runner.
type ReplySender struct {
	outbox       ReplyOutbox
	targets      ReplyTargetResolver
	providers    ReplyProviderResolver
	owner        string
	lease        time.Duration
	sendTimeout  time.Duration
	pollInterval time.Duration
	batchSize    int
	maxAttempts  int
}

// NewReplySender creates a finite-retry Reply Outbox sender.
func NewReplySender(
	outbox ReplyOutbox,
	targets ReplyTargetResolver,
	providers ReplyProviderResolver,
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

// Run delivers replies until ctx is canceled. It is the only long-lived
// lifecycle in the IM reply path; provider clients remain single-call values.
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
	sendCtx, cancel := context.WithTimeout(ctx, s.sendTimeout)
	defer cancel()
	provider, err := s.providers.ResolveReplyProvider(sendCtx, delivery)
	if err != nil {
		retryable, errorType := classifyReplyError(nil, err)
		if errors.Is(err, ErrReplyBindingInactive) {
			retryable = true
			errorType = "binding_inactive"
		} else if errorType == "provider_permanent" {
			errorType = "provider_resolution"
		}
		return s.recordFailure(ctx, delivery, err, retryable, errorType)
	}
	if err := provider.Capability.Validate(); err != nil {
		return s.recordFailure(ctx, delivery, err, false, "provider_capability")
	}
	if provider.Client == nil {
		return s.recordFailure(ctx, delivery, errors.New("reply provider client is not initialized"), false, "provider_client")
	}
	reply, err := prepareReply(delivery.Reply, provider.Capability)
	if err != nil {
		return s.recordFailure(ctx, delivery, err, false, "unsupported_reply")
	}
	delivery.Reply = reply
	if provider.Limiter != nil {
		if err := provider.Limiter.Allow(sendCtx, delivery); err != nil {
			return s.recordFailure(ctx, delivery, err, true, "rate_limit")
		}
	}
	providerTarget, outboundContext, err := s.targets.ResolveReplyTarget(sendCtx, delivery)
	if err != nil {
		return s.recordFailure(ctx, delivery, err, false, "target_resolution")
	}
	if outboundContext.ProviderMessageID == "" {
		outboundContext.ProviderMessageID = delivery.ProviderMessageID
	}
	if provider.Capability.SupportsStreaming &&
		(provider.Capability.RequiresInitialStreamResponse || reply.Operation != channels.ReplyOperationSend) &&
		outboundContext.StreamContext == "" {
		outboundContext.StreamContext = reply.LogicalReplyID
	}
	receipt, err := provider.Client.SendOnce(sendCtx, reply, providerTarget, outboundContext)
	if err != nil {
		retryable, errorType := classifyReplyError(provider.Classifier, err)
		return s.recordFailure(ctx, delivery, err, retryable, errorType)
	}
	if err := receipt.Validate(); err != nil {
		return s.recordFailure(ctx, delivery, err, false, "invalid_provider_receipt")
	}
	if err := s.outbox.CompleteReply(ctx, delivery, receipt); err != nil {
		return fmt.Errorf("complete reply: %w", err)
	}
	return nil
}

func (s *ReplySender) recordFailure(
	ctx context.Context,
	delivery ReplyDelivery,
	cause error,
	retryable bool,
	errorType string,
) error {
	if errorType == "" {
		errorType = "provider_send"
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

func classifyReplyError(classifier ReplyErrorClassifier, err error) (bool, string) {
	if classifier != nil {
		retryable, errorType := classifier.Classify(err)
		if errorType != "" {
			return retryable, errorType
		}
		return retryable, "provider_send"
	}
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

func prepareReply(reply channels.Reply, capability channels.ProviderCapability) (channels.Reply, error) {
	if reply.Operation == channels.ReplyOperationUpdate && !capability.SupportsUpdate {
		return channels.Reply{}, errors.New("provider does not support reply update")
	}
	if reply.Operation == channels.ReplyOperationFinalize && !capability.SupportsFinalize {
		if capability.SupportsUpdate {
			reply.Operation = channels.ReplyOperationUpdate
		} else {
			return channels.Reply{}, errors.New("provider does not support reply finalize")
		}
	}
	if reply.Kind == channels.ReplyKindCard && !capability.SupportsCard {
		reply = fallbackReply(reply, "card replies are not supported by this channel")
	}
	if reply.Kind == channels.ReplyKindArtifact && !capability.SupportsArtifact {
		reply = fallbackReply(reply, "attachment replies are not supported by this channel")
	}
	if reply.Kind == channels.ReplyKindText || reply.Kind == channels.ReplyKindFallbackText {
		if len([]byte(reply.Text)) > capability.MaxTextSize {
			return channels.Reply{}, errors.New("reply exceeds provider text limit")
		}
	}
	return reply, reply.Validate()
}

func fallbackReply(reply channels.Reply, defaultText string) channels.Reply {
	if strings.TrimSpace(reply.Text) == "" {
		reply.Text = defaultText
	}
	reply.Kind = channels.ReplyKindFallbackText
	reply.Card = nil
	reply.ArtifactRef = ""
	return reply
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

func visibleAssistantText(evt *event.Event) (string, bool) {
	if evt == nil || evt.Response == nil {
		return "", false
	}
	isDelta := evt.Response.IsPartial
	var builder strings.Builder
	for _, choice := range evt.Response.Choices {
		message := choice.Message
		if isDelta {
			message = choice.Delta
		} else if message.Content == "" {
			// Preserve compatibility with integrations that omit IsPartial while
			// exposing only the incremental Delta field.
			message = choice.Message
			if message.Content == "" {
				message = choice.Delta
			}
		}
		if message.Role != "" && message.Role != model.RoleAssistant {
			continue
		}
		if message.Content != "" {
			builder.WriteString(message.Content)
		}
	}
	return builder.String(), isDelta
}

func splitReplyText(value string, maxBytes int) ([]string, error) {
	if maxBytes <= 0 {
		return nil, errors.New("reply max text size must be positive")
	}
	if !utf8.ValidString(value) {
		return nil, errors.New("reply text is not valid utf-8")
	}
	remaining := value
	parts := make([]string, 0, 1)
	for remaining != "" {
		if len([]byte(remaining)) <= maxBytes {
			parts = append(parts, remaining)
			break
		}
		cut := maxBytes
		for cut > 0 && !utf8.ValidString(remaining[:cut]) {
			cut--
		}
		if cut == 0 {
			return nil, errors.New("reply max text size splits utf-8 sequence")
		}
		parts = append(parts, remaining[:cut])
		remaining = remaining[cut:]
	}
	if len(parts) == 0 {
		return []string{""}, nil
	}
	return parts, nil
}

func stableReplyID(reply channels.Reply) string {
	identity := strings.Join([]string{
		reply.TenantID, reply.AppID, reply.BindingID, reply.RequestID,
		reply.SourceEventID, reply.LogicalReplyID,
		fmt.Sprintf("%d", reply.PartNo), fmt.Sprintf("%d", reply.Revision), string(reply.Operation),
	}, "\x1f")
	return uuid.NewSHA1(uuid.Nil, []byte(identity)).String()
}
