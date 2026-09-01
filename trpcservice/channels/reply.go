package channels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// ReplyOperation identifies one platform-level outbound operation.
type ReplyOperation string

const (
	// ReplyOperationSend creates a new provider message.
	ReplyOperationSend ReplyOperation = "SEND"
	// ReplyOperationUpdate updates an existing provider message.
	ReplyOperationUpdate ReplyOperation = "UPDATE"
	// ReplyOperationFinalize finishes a streaming provider message.
	ReplyOperationFinalize ReplyOperation = "FINALIZE"
)

// ReplyKind identifies the platform-neutral content shape of a reply.
type ReplyKind string

const (
	// ReplyKindText is a plain text reply.
	ReplyKindText ReplyKind = "text"
	// ReplyKindCard is a platform-neutral or approved opaque card payload.
	ReplyKindCard ReplyKind = "card"
	// ReplyKindArtifact is a tenant-scoped artifact reply.
	ReplyKindArtifact ReplyKind = "artifact"
	// ReplyKindFallbackText is an explicit downgrade from an unsupported type.
	ReplyKindFallbackText ReplyKind = "fallback_text"
)

// ReplyTarget identifies the internal platform record from which a provider
// target is resolved. It never contains a raw provider identifier.
type ReplyTarget struct {
	Kind             TargetKind
	InternalEntityID string
}

// Validate checks the target reference shape.
func (t ReplyTarget) Validate() error {
	if t.Kind != TargetKindUser && t.Kind != TargetKindConversation && t.Kind != TargetKindTopic && t.Kind != TargetKindMessage {
		return errors.New("reply target kind is invalid")
	}
	if t.InternalEntityID == "" {
		return errors.New("reply target internal entity id is required")
	}
	return nil
}

// Reply is the provider-neutral content passed from Reply Projection to one
// provider outbound call. Claiming, persistence, and retry are owned by
// IM-06, not by this value or a ProviderOutboundClient.
type Reply struct {
	TenantID        string
	AppID           string
	RequestID       string
	SourceEventID   string
	Channel         Channel
	BindingID       string
	BindingRevision int64
	ReplyID         string
	LogicalReplyID  string
	PartNo          int64
	Revision        int64
	Operation       ReplyOperation
	Kind            ReplyKind
	Target          ReplyTarget
	Text            string
	// ContentDelta marks Text as an incremental Runner response. The durable
	// projection combines it with the previous visible content before it is
	// persisted for provider update/finalize operations.
	ContentDelta bool
	Card         json.RawMessage
	ArtifactRef  string
}

// Validate checks the stable platform-level fields required by an outbound
// reply operation.
func (r Reply) Validate() error {
	if r.TenantID == "" || r.AppID == "" {
		return errors.New("reply scope is required")
	}
	if r.RequestID == "" || r.SourceEventID == "" || r.ReplyID == "" || r.LogicalReplyID == "" {
		return errors.New("reply identity is required")
	}
	if err := r.Channel.Validate(); err != nil {
		return err
	}
	if r.BindingID == "" {
		return errors.New("reply binding_id is required")
	}
	if r.PartNo <= 0 || r.Revision <= 0 || r.BindingRevision <= 0 {
		return errors.New("reply part and revision must be positive")
	}
	switch r.Operation {
	case ReplyOperationSend, ReplyOperationUpdate, ReplyOperationFinalize:
	default:
		return errors.New("reply operation is invalid")
	}
	switch r.Kind {
	case ReplyKindText, ReplyKindFallbackText:
		if r.Text == "" {
			return errors.New("reply text is required")
		}
	case ReplyKindCard:
		if len(r.Card) == 0 || !json.Valid(r.Card) || r.Card[0] != '{' {
			return errors.New("reply card must be a json object")
		}
	case ReplyKindArtifact:
		if r.ArtifactRef == "" {
			return errors.New("reply artifact_ref is required")
		}
	default:
		return errors.New("reply kind is invalid")
	}
	return r.Target.Validate()
}

// ProviderCapability declares the outbound features and limits of one bound
// provider account.
type ProviderCapability struct {
	SupportsUpdate                bool
	SupportsCard                  bool
	SupportsArtifact              bool
	SupportsStreaming             bool
	RequiresInitialStreamResponse bool
	SupportsFinalize              bool
	MaxTextSize                   int
	TargetTTL                     time.Duration
}

// Validate checks capability values before an adapter is registered.
func (c ProviderCapability) Validate() error {
	if c.MaxTextSize <= 0 {
		return errors.New("provider max text size must be positive")
	}
	if c.TargetTTL < 0 {
		return errors.New("provider target ttl must not be negative")
	}
	if c.RequiresInitialStreamResponse && !c.SupportsStreaming {
		return errors.New("initial stream response requires streaming support")
	}
	if c.SupportsFinalize && !c.SupportsStreaming {
		return errors.New("finalize support requires streaming support")
	}
	return nil
}

// OutboundContext contains controlled state needed by one provider operation.
// It is not persisted as a platform payload and must not be logged.
type OutboundContext struct {
	ProviderMessageID string
	StreamContext     string
}

// ProviderReceipt contains the scoped delivery receipt returned by one
// provider operation.
type ProviderReceipt struct {
	ProviderMessageID string
}

// ProviderOutboundClient performs exactly one provider outbound operation.
// The caller resolves providerTarget immediately before the call; this
// interface does not claim, persist, classify, or retry Reply Outbox records.
type ProviderOutboundClient interface {
	SendOnce(
		ctx context.Context,
		reply Reply,
		providerTarget string,
		outboundContext OutboundContext,
	) (ProviderReceipt, error)
}

// ValidateProviderReceipt checks a receipt returned by a provider operation.
func (r ProviderReceipt) Validate() error {
	if r.ProviderMessageID == "" {
		return fmt.Errorf("provider message id is required")
	}
	return nil
}
