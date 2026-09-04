// Package outbound selects and creates the concrete IM reply provider.
package outbound

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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
	mu      sync.Mutex
	wecom   map[replyBindingKey]*wecom.OutboundClient
}

type replyBindingKey struct {
	tenantID        string
	appID           string
	bindingID       string
	bindingRevision int64
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
	return &Resolver{
		store:   store,
		secrets: secrets,
		limiter: limiter,
		wecom:   make(map[replyBindingKey]*wecom.OutboundClient),
	}, nil
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
		key := replyBindingKey{
			tenantID:        binding.TenantID,
			appID:           binding.AppID,
			bindingID:       binding.BindingID,
			bindingRevision: binding.BindingRevision,
		}
		r.mu.Lock()
		client := r.wecom[key]
		if client == nil {
			client, err = wecom.NewOutboundClient(ctx, r.secrets, binding.Snapshot())
			if err == nil {
				r.wecom[key] = client
			}
		}
		r.mu.Unlock()
		if err != nil {
			return worker.ReplyProvider{}, err
		}
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

// Close stops cached binding-scoped WeCom senders. Reply Outbox state remains
// owned by ReplySender; this only releases provider connections.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	clients := make([]*wecom.OutboundClient, 0, len(r.wecom))
	for _, client := range r.wecom {
		clients = append(clients, client)
	}
	r.wecom = make(map[replyBindingKey]*wecom.OutboundClient)
	r.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var result error
	for _, client := range clients {
		result = errors.Join(result, client.Close(ctx))
	}
	return result
}
