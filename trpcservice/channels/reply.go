package channels

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
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

// Reply is the durable text passed from Reply Projection to one provider
// outbound call.
type Reply struct {
	TenantID        string
	AppID           string
	RequestID       string
	SourceEventID   string
	Channel         Channel
	BindingID       string
	BindingRevision int64
	ReplyID         string
	Revision        int64
	Target          ReplyTarget
	Text            string
}

// StableID returns the deterministic identity shared by projection and the
// Reply Outbox, so event replay cannot create a second send.
func (r Reply) StableID() string {
	identity := strings.Join([]string{
		r.TenantID, r.AppID, r.BindingID, r.RequestID,
		r.SourceEventID, strconv.FormatInt(r.Revision, 10),
	}, "\x1f")
	return uuid.NewSHA1(uuid.Nil, []byte(identity)).String()
}

// Validate checks the platform-level fields required by a text reply.
func (r Reply) Validate() error {
	if r.TenantID == "" || r.AppID == "" {
		return errors.New("reply scope is required")
	}
	if r.RequestID == "" || r.SourceEventID == "" || r.ReplyID == "" {
		return errors.New("reply identity is required")
	}
	if err := r.Channel.Validate(); err != nil {
		return err
	}
	if r.BindingID == "" {
		return errors.New("reply binding_id is required")
	}
	if r.Revision <= 0 || r.BindingRevision <= 0 {
		return errors.New("reply revision must be positive")
	}
	if r.Text == "" {
		return errors.New("reply text is required")
	}
	return r.Target.Validate()
}

// ProviderOutboundClient performs exactly one ordinary text send. The caller
// resolves the provider target immediately before this call.
type ProviderOutboundClient interface {
	SendOnce(context.Context, Reply, string) (ProviderReceipt, error)
}

// ProviderReceipt contains the scoped delivery receipt returned by one
// provider operation.
type ProviderReceipt struct {
	ProviderMessageID string
}

// ValidateProviderReceipt checks a receipt returned by a provider operation.
func (r ProviderReceipt) Validate() error {
	if r.ProviderMessageID == "" {
		return fmt.Errorf("provider message id is required")
	}
	return nil
}
