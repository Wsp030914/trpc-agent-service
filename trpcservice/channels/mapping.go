package channels

import (
	"errors"
)

const (
	// DefaultSessionID is the session lane used by all v1 IM mappings.
	DefaultSessionID = "default"

	// NoThreadExternalID is the typed input used for conversations without a
	// provider thread. It is hashed in a namespace distinct from real threads.
	NoThreadExternalID = "thread:none"

	// ConversationScopeGroup identifies a conversation shared by a group.
	ConversationScopeGroup = "group"
	// ConversationScopeTopic identifies a topic or thread conversation.
	ConversationScopeTopic = "topic"
)

// MappedPrincipal is the internal identity and session mapping for one channel
// message. Direct messages have no Conversation or Membership.
type MappedPrincipal struct {
	Identity           Identity
	Conversation       *Conversation
	Membership         *Membership
	SessionPrincipalID string
	SessionID          string
}

// Validate checks the session mapping invariants for a direct, group, or topic
// message.
func (m MappedPrincipal) Validate() error {
	if m.Identity.UserID == "" {
		return errors.New("mapped identity user_id is required")
	}
	if m.SessionPrincipalID == "" {
		return errors.New("mapped session principal is required")
	}
	if m.SessionID == "" {
		return errors.New("mapped session_id is required")
	}
	if m.Conversation == nil {
		if m.SessionPrincipalID != m.Identity.UserID {
			return errors.New("direct session principal must equal user_id")
		}
		if m.Membership != nil {
			return errors.New("direct mapping cannot have membership")
		}
		return nil
	}
	if m.Conversation.ConversationID == "" {
		return errors.New("mapped conversation_id is required")
	}
	if m.SessionPrincipalID != m.Conversation.SessionPrincipalID {
		return errors.New("shared session principal does not match conversation")
	}
	if m.Membership == nil {
		return errors.New("shared mapping membership is required")
	}
	if m.Membership.UserID != m.Identity.UserID || m.Membership.ConversationID != m.Conversation.ConversationID {
		return errors.New("mapping membership does not match principal")
	}
	return nil
}
