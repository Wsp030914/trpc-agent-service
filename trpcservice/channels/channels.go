// Package channels models tenant-owned IM channel bindings.
package channels

import (
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Channel identifies an external IM platform.
type Channel string

const (
	// ChannelWeCom identifies Enterprise WeChat.
	ChannelWeCom Channel = "wecom"
	// ChannelWeChatCustomer identifies WeChat customer service.
	ChannelWeChatCustomer Channel = "wechat_customer"
	// ChannelTelegram identifies Telegram.
	ChannelTelegram Channel = "telegram"
)

// BindingStatus is the lifecycle state of an IM channel binding.
type BindingStatus string

const (
	// BindingActive accepts inbound callbacks and outbound replies.
	BindingActive BindingStatus = "ACTIVE"
	// BindingSuspended rejects new inbound callbacks.
	BindingSuspended BindingStatus = "SUSPENDED"
)

// Binding maps one verified external IM account to a tenant application.
type Binding struct {
	TenantID         string
	AppID            string
	BindingID        string
	Channel          Channel
	ExternalAccount  string
	WebhookURL       string
	TokenRef         tenant.SecretRef
	SigningSecretRef tenant.SecretRef
	Secret           tenant.SecretRef
	Status           BindingStatus
}

// Scope returns the tenant application scope that owns the binding.
func (b Binding) Scope() tenant.Scope {
	return tenant.Scope{TenantID: b.TenantID, AppID: b.AppID}
}

// Validate checks that the binding can be used as a trusted tenant source.
func (b Binding) Validate() error {
	if b.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if b.AppID == "" {
		return errors.New("app_id is required")
	}
	if b.BindingID == "" {
		return errors.New("binding_id is required")
	}
	if !validChannel(b.Channel) {
		return errors.New("channel is invalid")
	}
	if b.ExternalAccount == "" {
		return errors.New("external_account is required")
	}
	if b.WebhookURL == "" {
		return errors.New("webhook_url is required")
	}
	if err := b.TokenRef.Validate(); err != nil {
		return fmt.Errorf("token_ref: %w", err)
	}
	if err := b.SigningSecretRef.Validate(); err != nil {
		return fmt.Errorf("signing_secret_ref: %w", err)
	}
	if !secretRefZero(b.Secret) {
		if err := b.Secret.Validate(); err != nil {
			return fmt.Errorf("secret_ref: %w", err)
		}
	}
	if !validBindingStatus(b.Status) {
		return errors.New("binding status is invalid")
	}
	return nil
}

// Identity maps a platform user into the internal user model.
type Identity struct {
	TenantID            string
	AppID               string
	BindingID           string
	Channel             Channel
	ExternalUserKeyHash string
	UserID              string
	Status              string
	KeyVersion          string
}

// Conversation maps an external chat or thread into a session principal.
type Conversation struct {
	TenantID            string
	AppID               string
	BindingID           string
	Channel             Channel
	ExternalChatKeyHash string
	ThreadKeyHash       string
	ConversationID      string
	SessionPrincipalID  string
	Scope               string
}

// Membership records a user's role in one IM conversation.
type Membership struct {
	TenantID       string
	AppID          string
	ConversationID string
	UserID         string
	Role           string
	Status         string
}

func validChannel(channel Channel) bool {
	switch channel {
	case ChannelWeCom, ChannelWeChatCustomer, ChannelTelegram:
		return true
	default:
		return false
	}
}

func validBindingStatus(status BindingStatus) bool {
	return status == BindingActive || status == BindingSuspended
}

func secretRefZero(ref tenant.SecretRef) bool {
	return ref.Name == "" && ref.Version == ""
}
