package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	defaultMaxCallbackBytes = 1 << 20
	defaultClockSkew        = 5 * time.Minute
	feishuMessageTargetTTL  = time.Hour
)

var (
	errInvalidFeishuPath     = errors.New("invalid feishu callback path")
	errCallbackBodyRequired  = errors.New("feishu callback body is required")
	errCallbackBodyTooLarge  = errors.New("feishu callback body is too large")
	errCallbackContentType   = errors.New("feishu callback content type is invalid")
	errCallbackUnavailable   = errors.New("feishu callback verification unavailable")
	errAttachmentIngestorReq = errors.New("attachment ingestor is required")
)

// AdapterOption configures a Feishu callback Adapter.
type AdapterOption func(*Adapter) error

// WithMaxCallbackBytes sets the maximum callback body size accepted by the
// adapter.
func WithMaxCallbackBytes(limit int64) AdapterOption {
	return func(adapter *Adapter) error {
		if limit <= 0 {
			return errors.New("max callback bytes must be positive")
		}
		adapter.maxCallbackBytes = limit
		return nil
	}
}

// WithClock supplies the clock used for callback freshness and reply-target expiry.
func WithClock(clock func() time.Time) AdapterOption {
	return func(adapter *Adapter) error {
		if clock == nil {
			return errors.New("clock is required")
		}
		adapter.now = clock
		return nil
	}
}

// WithClockSkew sets the maximum accepted difference between the Feishu
// request timestamp and the adapter clock.
func WithClockSkew(skew time.Duration) AdapterOption {
	return func(adapter *Adapter) error {
		if skew <= 0 {
			return errors.New("clock skew must be positive")
		}
		adapter.clockSkew = skew
		return nil
	}
}

// WithAttachmentIngestor registers the IM-07 media materialization boundary.
// Verified media callbacks are rejected for retry when no ingestor is set.
func WithAttachmentIngestor(ingestor channels.AttachmentIngestor) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.attachmentIngestor = ingestor
		return nil
	}
}

// WithRecallAdmitter registers the durable recall boundary. Authenticated
// recall events are rejected for retry when no admitter is configured.
func WithRecallAdmitter(admitter channels.RecallAdmitter) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.recallAdmitter = admitter
		return nil
	}
}

// Adapter verifies Feishu callbacks and submits normalized ChannelInput
// values to Gateway. It does not call Runner or own Reply Outbox delivery.
type Adapter struct {
	routes             gateway.PublicRouteResolver
	admissionGateway   *gateway.Gateway
	secrets            platformsecret.SecretProvider
	attachmentIngestor channels.AttachmentIngestor
	recallAdmitter     channels.RecallAdmitter
	maxCallbackBytes   int64
	clockSkew          time.Duration
	now                func() time.Time
}

// NewAdapter creates a Feishu callback Adapter. Binding.TokenRef is the
// Feishu Verification Token reference and Binding.SigningSecretRef is the
// Feishu Encrypt Key reference. Binding.Secret is reserved for the outbound
// App Secret and is resolved by NewOutboundClient.
func NewAdapter(
	routes gateway.PublicRouteResolver,
	admissionGateway *gateway.Gateway,
	secrets platformsecret.SecretProvider,
	opts ...AdapterOption,
) (*Adapter, error) {
	if routes == nil {
		return nil, errors.New("public route resolver is required")
	}
	if admissionGateway == nil {
		return nil, errors.New("gateway is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	adapter := &Adapter{
		routes:           routes,
		admissionGateway: admissionGateway,
		secrets:          secrets,
		maxCallbackBytes: defaultMaxCallbackBytes,
		clockSkew:        defaultClockSkew,
		now:              time.Now,
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(adapter); err != nil {
			return nil, err
		}
	}
	return adapter, nil
}

// ServeHTTP handles the bound Feishu callback path. The route key is the
// only caller-supplied routing value; tenant and application scope come from
// the Binding returned by the route resolver.
func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a == nil || w == nil || r == nil {
		return
	}
	routeKey, err := callbackRouteKey(r)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	route, err := gateway.ResolveChannelBindingRoute(
		r.Context(),
		a.routes,
		channels.ChannelFeishu,
		routeKey,
	)
	if err != nil {
		writeRouteError(w, r, err)
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	a.handleCallback(w, r, route)
}

func (a *Adapter) handleCallback(w http.ResponseWriter, r *http.Request, route gateway.LocatedChannelBinding) {
	if r.Body == nil {
		writeProtocolError(w, r, errCallbackBodyRequired)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()
	if err := validateJSONContentType(r); err != nil {
		writeProtocolError(w, r, err)
		return
	}
	body, err := readCallbackBody(w, r, a.maxCallbackBytes)
	if err != nil {
		writeProtocolError(w, r, err)
		return
	}
	binding := route.Snapshot()
	verificationToken, encryptKey, err := a.callbackSecrets(r.Context(), binding)
	if err != nil {
		http.Error(w, "callback verification unavailable", http.StatusServiceUnavailable)
		return
	}
	plain, metadata, err := decodeCallback(body, r.Header, encryptKey, a.now().UTC(), a.clockSkew)
	if err != nil {
		writeProtocolError(w, r, err)
		return
	}
	if err := verifyCallbackMetadata(metadata, binding, verificationToken); err != nil {
		writeProtocolError(w, r, err)
		return
	}
	if metadata.RequestType == feishuChallengeType {
		writeChallenge(w, metadata.Challenge)
		return
	}
	if metadata.EventType == feishuRecallEventType {
		if a.recallAdmitter == nil {
			http.Error(w, "recall processing unavailable", http.StatusServiceUnavailable)
			return
		}
		recall, err := normalizeRecallEvent(binding, metadata, plain)
		if err != nil {
			writeProtocolError(w, r, err)
			return
		}
		if _, err := a.recallAdmitter.AdmitRecall(r.Context(), recall); err != nil {
			writeAdmissionError(w, r, err)
			return
		}
		writeEventAck(w)
		return
	}
	if metadata.EventType != feishuMessageEventType {
		// The callback has been authenticated, but this task only admits
		// message receive events. Acknowledge other subscribed events so the
		// provider does not retry them indefinitely.
		writeEventAck(w)
		return
	}
	var received larkim.P2MessageReceiveV1
	if err := json.Unmarshal(plain, &received); err != nil {
		writeProtocolError(w, r, fmt.Errorf("%w: typed event", errFeishuCallbackEvent))
		return
	}
	envelope, err := normalizeMessageEvent(binding, &received)
	if err != nil {
		writeProtocolError(w, r, err)
		return
	}
	input, err := a.channelInput(r.Context(), envelope)
	if err != nil {
		if errors.Is(err, errAttachmentIngestorReq) {
			http.Error(w, "attachment processing unavailable", http.StatusServiceUnavailable)
			return
		}
		writeProtocolError(w, r, err)
		return
	}
	requestID := uuid.NewString()
	runtimeContext := tenant.RuntimeContext{
		TenantID:  binding.TenantID,
		AppID:     binding.AppID,
		Channel:   string(binding.Channel),
		BindingID: binding.BindingID,
		SessionID: channels.DefaultSessionID,
		TraceID:   requestID,
	}
	identityResolver, err := gateway.NewChannelBindingInputIdentityResolver(route, runtimeContext)
	if err != nil {
		http.Error(w, "callback admission unavailable", http.StatusServiceUnavailable)
		return
	}
	_, err = a.admissionGateway.Handle(r.Context(), gateway.Request{
		RequestID:      requestID,
		IdempotencyKey: envelope.ExternalMessageID,
		Tenant:         identityResolver,
		ChannelInput:   &input,
	})
	if err != nil {
		writeAdmissionError(w, r, err)
		return
	}
	writeEventAck(w)
}

func (a *Adapter) callbackSecrets(ctx context.Context, binding channels.BindingSnapshot) (string, string, error) {
	scope := binding.Scope()
	verificationToken, err := a.secrets.ResolveSecret(ctx, scope, binding.TokenRef)
	if err != nil || verificationToken == "" {
		if err != nil {
			return "", "", fmt.Errorf("resolve feishu verification token: %w", err)
		}
		return "", "", errCallbackUnavailable
	}
	encryptKey, err := a.secrets.ResolveSecret(ctx, scope, binding.SigningSecretRef)
	if err != nil {
		return "", "", fmt.Errorf("resolve feishu encrypt key: %w", err)
	}
	return verificationToken, encryptKey, nil
}

func (a *Adapter) channelInput(ctx context.Context, envelope VerifiedProviderEnvelope) (channels.ChannelInput, error) {
	if err := envelope.Validate(); err != nil {
		return channels.ChannelInput{}, err
	}
	input := channels.ChannelInput{
		TenantID:          envelope.TenantID,
		AppID:             envelope.AppID,
		Channel:           envelope.Channel,
		BindingID:         envelope.BindingID,
		BindingRevision:   envelope.BindingRevision,
		ExternalMessageID: envelope.ExternalMessageID,
		Conversation: channels.ChannelConversation{
			Kind: envelope.ConversationKind,
		},
		MessageType:       envelope.MessageType,
		Text:              envelope.Text,
		ProviderTimestamp: envelope.ProviderTimestamp,
		ReceivedAt:        a.now().UTC(),
	}
	input, err := channels.NewChannelInput(input, envelope.mapping)
	if err != nil {
		return channels.ChannelInput{}, err
	}
	if len(envelope.Media) > 0 {
		if a.attachmentIngestor == nil {
			return channels.ChannelInput{}, errAttachmentIngestorReq
		}
		media := append([]channels.ProviderMediaRef(nil), envelope.Media...)
		input, err = a.attachmentIngestor.Prepare(ctx, input, media)
		if err != nil {
			return channels.ChannelInput{}, fmt.Errorf("prepare feishu media: %w", err)
		}
	}
	input, err = channels.WithMessageReplyTarget(input, channels.MessageReplyTarget{
		ProviderTarget: envelope.replyTarget,
		ExpiresAt:      a.now().UTC().Add(feishuMessageTargetTTL),
	})
	if err != nil {
		return channels.ChannelInput{}, err
	}
	return input, nil
}

func callbackRouteKey(r *http.Request) (string, error) {
	if r == nil || r.URL == nil {
		return "", errInvalidFeishuPath
	}
	path := strings.TrimSuffix(r.URL.Path, "/")
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "im" || parts[1] != string(channels.ChannelFeishu) || parts[2] == "" {
		return "", errInvalidFeishuPath
	}
	return parts[2], nil
}

func validateJSONContentType(r *http.Request) error {
	value := strings.TrimSpace(r.Header.Get("Content-Type"))
	if value == "" {
		return errCallbackContentType
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil || mediaType != "application/json" {
		return errCallbackContentType
	}
	return nil
}

func readCallbackBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errCallbackBodyTooLarge
	}
	limited := http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, errCallbackBodyTooLarge
		}
		return nil, errCallbackBodyRequired
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, errCallbackBodyRequired
	}
	return body, nil
}

func writeChallenge(w http.ResponseWriter, challenge string) {
	payload, err := json.Marshal(struct {
		Challenge string `json:"challenge"`
	}{Challenge: challenge})
	if err != nil {
		http.Error(w, "callback verification unavailable", http.StatusInternalServerError)
		return
	}
	writeJSON(w, http.StatusOK, payload)
}

func writeEventAck(w http.ResponseWriter) {
	writeJSON(w, http.StatusOK, []byte(`{"code":0}`))
}

func writeJSON(w http.ResponseWriter, status int, payload []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
}

func writeRouteError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, channels.ErrBindingNotFound), errors.Is(err, channels.ErrBindingChannelMismatch):
		http.NotFound(w, r)
	case errors.Is(err, channels.ErrBindingInactive):
		http.Error(w, "callback binding is inactive", http.StatusForbidden)
	default:
		http.Error(w, "callback route unavailable", http.StatusServiceUnavailable)
	}
}

func writeProtocolError(w http.ResponseWriter, _ *http.Request, err error) {
	if errors.Is(err, errCallbackBodyTooLarge) {
		http.Error(w, "callback body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "invalid callback", http.StatusBadRequest)
}

func writeAdmissionError(w http.ResponseWriter, _ *http.Request, err error) {
	switch {
	case errors.Is(err, gateway.ErrIdempotencyConflict):
		http.Error(w, "callback idempotency conflict", http.StatusConflict)
	case errors.Is(err, channels.ErrBindingInactive), errors.Is(err, gateway.ErrChannelBindingSnapshotStale):
		http.Error(w, "callback binding changed", http.StatusServiceUnavailable)
	default:
		http.Error(w, "callback admission unavailable", http.StatusServiceUnavailable)
	}
}
