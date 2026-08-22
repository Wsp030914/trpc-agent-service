// Package channels models tenant-owned IM channel bindings.
package channels

import "github.com/liuzengh/trpc-agent-service/trpcservice/tenant"

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
	TenantID        string
	AppID           string
	BindingID       string
	Channel         Channel
	ExternalAccount string
	Secret          tenant.SecretRef
	Status          BindingStatus
}

// Scope returns the tenant application scope that owns the binding.
func (b Binding) Scope() tenant.Scope {
	return tenant.Scope{TenantID: b.TenantID, AppID: b.AppID}
}

// Identity maps a platform user into the internal actor model.
type Identity struct {
	TenantID            string
	AppID               string
	BindingID           string
	Channel             Channel
	ExternalUserKeyHash string
	ActorUserID         string
	PrincipalID         string
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

// Membership records an actor's role in one IM conversation.
type Membership struct {
	TenantID       string
	AppID          string
	ConversationID string
	ActorUserID    string
	Role           string
	Status         string
}
