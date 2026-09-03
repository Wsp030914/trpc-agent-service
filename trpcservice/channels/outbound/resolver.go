// Package outbound selects and creates the concrete IM reply provider.
package outbound

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// Resolver selects the concrete provider for one durable Reply Outbox row.
type Resolver struct {
	store   *postgres.Store
	secrets platformsecret.SecretProvider
	limiter worker.ReplyRateLimiter
}

// NewResolver creates an IM outbound resolver. Provider clients remain
// single-call values; the durable outbox owns retries and ordering.
func NewResolver(
	store *postgres.Store,
	secrets platformsecret.SecretProvider,
	limiter worker.ReplyRateLimiter,
) (*Resolver, error) {
	if store == nil || secrets == nil {
		return nil, errors.New("reply provider dependencies are required")
	}
	return &Resolver{store: store, secrets: secrets, limiter: limiter}, nil
}

// ResolveReplyProvider revalidates the binding revision before creating one
// provider client for the delivery.
func (r *Resolver) ResolveReplyProvider(
	ctx context.Context,
	delivery worker.ReplyDelivery,
) (worker.ReplyProvider, error) {
	if r == nil || r.store == nil || r.secrets == nil {
		return worker.ReplyProvider{}, errors.New("reply provider resolver is not initialized")
	}
	binding, err := r.store.ResolveBinding(
		ctx,
		delivery.Reply.TenantID,
		delivery.Reply.AppID,
		delivery.Reply.BindingID,
	)
	if err != nil {
		return worker.ReplyProvider{}, err
	}
	if binding.Status != channels.BindingActive {
		return worker.ReplyProvider{}, worker.ErrReplyBindingInactive
	}
	if binding.Channel != delivery.Reply.Channel || binding.BindingRevision != delivery.Reply.BindingRevision {
		return worker.ReplyProvider{}, worker.ErrReplyBindingChanged
	}
	switch binding.Channel {
	case channels.ChannelWeCom:
		client := wecom.NewOutboundClient(nil)
		return worker.ReplyProvider{Client: client, Limiter: r.limiter}, nil
	case channels.ChannelFeishu:
		client, err := feishu.NewOutboundClient(ctx, r.secrets, binding.Snapshot())
		if err != nil {
			return worker.ReplyProvider{}, err
		}
		return worker.ReplyProvider{Client: client, Limiter: r.limiter}, nil
	default:
		return worker.ReplyProvider{}, fmt.Errorf("unsupported reply channel %q", binding.Channel)
	}
}
