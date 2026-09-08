package wecom

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const maxWeComReplyBytes = 20480

var errWeComReplyTooLarge = errors.New("wecom reply is too large")

// ProviderSendError is the stable, body-redacted error returned by one
// WeCom protocol send. Reply Outbox owns retry and attempt state.
type ProviderSendError struct {
	Code            int
	Retryable       bool
	Uncertain       bool
	RetryAfterDelay time.Duration
	cause           error
}

func (e *ProviderSendError) Error() string {
	if e == nil {
		return "wecom provider send failed"
	}
	if e.Code != 0 {
		return fmt.Sprintf("wecom provider returned code %d", e.Code)
	}
	return "wecom provider send failed"
}

func (e *ProviderSendError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *ProviderSendError) IsRetryable() bool {
	return e != nil && e.Retryable
}

func (e *ProviderSendError) IsSideEffectUncertain() bool {
	return e != nil && e.Uncertain
}

func (e *ProviderSendError) RetryAfter() time.Duration {
	if e == nil || e.RetryAfterDelay < 0 {
		return 0
	}
	return e.RetryAfterDelay
}

// MessageSender is the provider operation used by Reply Outbox.
type MessageSender interface {
	SendMessage(context.Context, string, string) (string, error)
}

// OutboundOption configures one binding-scoped sender.
type OutboundOption func(*outboundConfig) error

type outboundConfig struct {
	sender        MessageSender
	clientOptions []ClientOption
}

// WithMessageSender injects a protocol sender for focused provider tests.
func WithMessageSender(sender MessageSender) OutboundOption {
	return func(config *outboundConfig) error {
		if sender == nil {
			return errors.New("wecom message sender is required")
		}
		config.sender = sender
		return nil
	}
}

// WithClientOptions passes protocol options to the binding-scoped client.
func WithClientOptions(options ...ClientOption) OutboundOption {
	return func(config *outboundConfig) error {
		config.clientOptions = append(config.clientOptions, options...)
		return nil
	}
}

// OutboundClient performs exactly one official aibot_send_msg operation. It
// does not claim, persist, retry, or otherwise manage Reply Outbox state.
type OutboundClient struct {
	sender MessageSender
}

// NewOutboundClient resolves the binding's WeCom Bot Secret and creates a
// binding-scoped long-connection sender. It must be constructed by the single
// Channel owner; the sender reconnects independently and is closed by the
// outbound resolver lifecycle.
func NewOutboundClient(
	ctx context.Context,
	secrets platformsecret.SecretProvider,
	binding channels.BindingSnapshot,
	opts ...OutboundOption,
) (*OutboundClient, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("wecom binding: %w", err)
	}
	if binding.Channel != channels.ChannelWeCom {
		return nil, errors.New("wecom outbound client received another channel")
	}
	if binding.Secret.Name == "" {
		return nil, errors.New("wecom bot secret reference is required")
	}
	botSecret, err := secrets.ResolveSecret(ctx, binding.Scope(), binding.Secret)
	if err != nil {
		return nil, fmt.Errorf("resolve wecom bot secret: %w", err)
	}
	if botSecret == "" {
		return nil, errors.New("wecom bot secret is required")
	}
	config := outboundConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, err
		}
	}
	if config.sender == nil {
		config.sender, err = NewClient(binding.ExternalAccount, botSecret, config.clientOptions...)
		if err != nil {
			return nil, err
		}
	}
	return &OutboundClient{sender: config.sender}, nil
}

// SendOnce sends one Reply to the resolved WeCom user ID or group chat ID.
func (c *OutboundClient) SendOnce(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.sender == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom outbound client is not initialized")}
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("wecom reply: %w", err)
	}
	if reply.Channel != channels.ChannelWeCom {
		return channels.ProviderReceipt{}, errors.New("wecom outbound client received another channel")
	}
	target, err := channels.NormalizeExternalID(providerTarget)
	if err != nil || target != providerTarget {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom provider target is invalid")}
	}
	if !utf8.ValidString(reply.Text) || len([]byte(reply.Text)) > maxWeComReplyBytes {
		return channels.ProviderReceipt{}, errWeComReplyTooLarge
	}
	providerMessageID, err := c.sender.SendMessage(ctx, target, reply.Text)
	if err != nil {
		return channels.ProviderReceipt{}, providerError(err)
	}
	if strings.TrimSpace(providerMessageID) == "" {
		providerMessageID = reply.ReplyID
	}
	return channels.ProviderReceipt{ProviderMessageID: providerMessageID}, nil
}

// Close releases a sender-owned long connection.
func (c *OutboundClient) Close(ctx context.Context) error {
	if c == nil || c.sender == nil {
		return nil
	}
	if client, ok := c.sender.(*WebSocketClient); ok {
		return client.Close(ctx)
	}
	return nil
}

func providerError(err error) error {
	if err == nil {
		return nil
	}
	var protocolErr *protocolError
	if errors.As(err, &protocolErr) {
		return &ProviderSendError{
			Code:      protocolErr.code,
			Retryable: retryableWeComCode(protocolErr.code),
			cause:     err,
		}
	}
	return &ProviderSendError{
		Retryable: !errors.Is(err, context.Canceled),
		Uncertain: !errors.Is(err, context.Canceled),
		cause:     err,
	}
}

func retryableWeComCode(code int) bool {
	return code == 45009 || code >= 50000
}

var _ channels.ProviderOutboundClient = (*OutboundClient)(nil)
