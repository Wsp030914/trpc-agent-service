package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/internal/callbackhttp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	defaultMaxCallbackBytes = 1 << 20
	defaultClockSkew        = 5 * time.Minute
	wecomMessageTargetTTL   = time.Hour
	maxProviderURLLength    = 16 << 10
)

var (
	errCallbackQuery                = errors.New("wecom callback query is invalid")
	errCallbackTimestamp            = errors.New("wecom callback timestamp is invalid")
	errCallbackMessage              = errors.New("wecom callback message is invalid")
	errWeComBindingAccountMismatch  = errors.New("wecom binding account does not match callback")
	errWeComMessageIDRequired       = errors.New("wecom message id is required")
	errWeComSenderRequired          = errors.New("wecom sender id is required")
	errWeComConversationInvalid     = errors.New("wecom conversation is invalid")
	errWeComResponseTargetInvalid   = errors.New("wecom response target is invalid")
	errAttachmentIngestorRequired   = errors.New("attachment ingestor is required")
	errWeComUnsupportedMediaPayload = errors.New("wecom media payload is invalid")
	errWeComMessageConversation     = errors.New("wecom message is invalid for conversation")
)

// AdapterOption configures an Enterprise WeChat callback Adapter.
type AdapterOption func(*Adapter) error

// WithMaxCallbackBytes sets the maximum encrypted callback body size.
func WithMaxCallbackBytes(limit int64) AdapterOption {
	return func(adapter *Adapter) error {
		if limit <= 0 {
			return errors.New("max callback bytes must be positive")
		}
		adapter.maxCallbackBytes = limit
		return nil
	}
}

// WithClock supplies the clock used for callback freshness and message target expiry.
func WithClock(clock func() time.Time) AdapterOption {
	return func(adapter *Adapter) error {
		if clock == nil {
			return errors.New("clock is required")
		}
		adapter.now = clock
		return nil
	}
}

// WithClockSkew sets the maximum accepted difference between provider and local time.
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
// Text callbacks do not need an ingestor; verified callbacks containing media
// are rejected for retry when one is not configured.
func WithAttachmentIngestor(ingestor channels.AttachmentIngestor) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.attachmentIngestor = ingestor
		return nil
	}
}

// Adapter verifies Enterprise WeChat AI Bot callbacks and submits normalized
// channel inputs to the tenant-aware Gateway. It does not call Runner or own
// reply outbox delivery.
type Adapter struct {
	routes             gateway.PublicRouteResolver
	admissionGateway   *gateway.Gateway
	secrets            platformsecret.SecretProvider
	attachmentIngestor channels.AttachmentIngestor
	maxCallbackBytes   int64
	clockSkew          time.Duration
	now                func() time.Time
}

// NewAdapter creates an Enterprise WeChat AI Bot callback Adapter.
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

// ServeHTTP handles the bound Enterprise WeChat callback path. The route key
// is the only caller-supplied routing value; tenant and application scope come
// from the Binding returned by the route resolver.
func (a *Adapter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if a == nil || w == nil || r == nil {
		return
	}
	routeKey, err := callbackhttp.RouteKey(r, channels.ChannelWeCom)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	route, err := gateway.ResolveChannelBindingRoute(
		r.Context(),
		a.routes,
		channels.ChannelWeCom,
		routeKey,
	)
	if err != nil {
		callbackhttp.WriteRouteError(w, r, err)
		return
	}
	switch r.Method {
	case http.MethodGet:
		a.handleURLVerification(w, r, route.Snapshot())
	case http.MethodPost:
		a.handleCallback(w, r, route)
	default:
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// VerifiedProviderEnvelope is the provider-neutral result of WeCom protocol
// verification. Its private mapping and reply target are consumed only by the
// adapter when constructing ChannelInput; raw WeCom DTO fields never cross the
// Gateway boundary.
type VerifiedProviderEnvelope struct {
	TenantID          string
	AppID             string
	Channel           channels.Channel
	BindingID         string
	BindingRevision   int64
	ExternalMessageID string
	SenderID          string
	ConversationKind  channels.ConversationKind
	ChatID            string
	ThreadID          string
	MessageType       channels.MessageType
	Text              string
	Media             []channels.ProviderMediaRef
	ProviderTimestamp time.Time
	Context           ProviderCallbackContext

	mapping     channels.ChannelMappingInput
	responseURL string
}

// ProviderCallbackContext keeps the small amount of verified WeCom context
// that is needed by a later provider-specific event or streaming handler. It
// is intentionally owned by this package and is never copied into Gateway,
// Worker, Runner, Session, or ordinary channel persistence.
type ProviderCallbackContext struct {
	StreamID     string
	EventType    string
	EventPayload []byte
}

// Validate checks the normalized fields needed to build a channel input.
func (e VerifiedProviderEnvelope) Validate() error {
	if e.TenantID == "" || e.AppID == "" {
		return errors.New("verified wecom envelope scope is required")
	}
	if e.Channel != channels.ChannelWeCom {
		return errors.New("verified wecom envelope channel is invalid")
	}
	if e.BindingID == "" || e.BindingRevision <= 0 {
		return errors.New("verified wecom envelope binding is invalid")
	}
	normalizedMessageID, err := channels.NormalizeExternalID(e.ExternalMessageID)
	if err != nil || normalizedMessageID != e.ExternalMessageID {
		return errWeComMessageIDRequired
	}
	normalizedSenderID, err := channels.NormalizeExternalID(e.SenderID)
	if err != nil || normalizedSenderID != e.SenderID {
		return errWeComSenderRequired
	}
	if err := e.ConversationKind.Validate(); err != nil {
		return err
	}
	switch e.ConversationKind {
	case channels.ConversationDirect:
		if e.ChatID != "" || e.ThreadID != "" {
			return errWeComConversationInvalid
		}
	case channels.ConversationGroup:
		if _, err := channels.NormalizeExternalID(e.ChatID); err != nil || e.ThreadID != "" {
			return errWeComConversationInvalid
		}
	default:
		return errWeComConversationInvalid
	}
	if err := e.MessageType.Validate(); err != nil {
		return err
	}
	if e.MessageType == channels.MessageTypeText && e.Text == "" {
		return errCallbackMessage
	}
	if e.MessageType == channels.MessageTypeMixed && e.Text == "" && len(e.Media) == 0 {
		return errCallbackMessage
	}
	if !utf8.ValidString(e.Text) {
		return errCallbackMessage
	}
	for _, media := range e.Media {
		if err := media.Validate(); err != nil {
			return fmt.Errorf("wecom media: %w", err)
		}
	}
	if e.Context.StreamID != "" && e.Context.EventType != "" {
		return errCallbackMessage
	}
	if e.MessageType == channels.MessageTypeEvent {
		if e.Context.StreamID == "" && e.Context.EventType == "" {
			return errCallbackMessage
		}
		if len(e.Context.EventPayload) > 0 && !json.Valid(e.Context.EventPayload) {
			return errCallbackMessage
		}
	} else if e.Context.StreamID != "" || e.Context.EventType != "" || len(e.Context.EventPayload) > 0 {
		return errCallbackMessage
	}
	if e.responseURL != "" {
		if err := validateResponseURL(e.responseURL); err != nil {
			return errWeComResponseTargetInvalid
		}
	}
	if err := e.mapping.Validate(e.ConversationKind); err != nil {
		return fmt.Errorf("wecom mapping: %w", err)
	}
	return nil
}

func (a *Adapter) handleURLVerification(w http.ResponseWriter, r *http.Request, binding channels.BindingSnapshot) {
	codec, err := a.codecFor(r.Context(), binding)
	if err != nil {
		http.Error(w, "callback verification unavailable", http.StatusServiceUnavailable)
		return
	}
	echostr, err := oneValue(r.URL.Query(), "echostr")
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	query, err := signedQuery(r, echostr)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	if err := checkTimestamp(query.timestamp, a.now(), a.clockSkew); err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	if err := codec.verifySignature(query.timestamp, query.nonce, query.encrypted, query.signature); err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	message, err := codec.decrypt(query.encrypted)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(message)
}

func (a *Adapter) handleCallback(w http.ResponseWriter, r *http.Request, route gateway.LocatedChannelBinding) {
	if r.Body == nil {
		callbackhttp.WriteProtocolError(w, callbackhttp.ErrBodyRequired)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()
	if err := callbackhttp.ValidateJSONContentType(r); err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	body, err := callbackhttp.ReadBody(w, r, a.maxCallbackBytes)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	encrypted, err := decodeEncryptedCallback(body)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	query, err := signedQuery(r, encrypted)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	if err := checkTimestamp(query.timestamp, a.now(), a.clockSkew); err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	binding := route.Snapshot()
	codec, err := a.codecFor(r.Context(), binding)
	if err != nil {
		http.Error(w, "callback verification unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := codec.verifySignature(query.timestamp, query.nonce, encrypted, query.signature); err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	decrypted, err := codec.decrypt(encrypted)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	var callback callbackMessage
	if err := json.Unmarshal(decrypted, &callback); err != nil {
		callbackhttp.WriteProtocolError(w, fmt.Errorf("%w: json", errCallbackMessage))
		return
	}
	envelope, err := normalizeCallback(binding, callback)
	if err != nil {
		callbackhttp.WriteProtocolError(w, err)
		return
	}
	input, err := a.channelInput(r.Context(), envelope)
	if err != nil {
		if errors.Is(err, errAttachmentIngestorRequired) {
			http.Error(w, "attachment processing unavailable", http.StatusServiceUnavailable)
			return
		}
		callbackhttp.WriteProtocolError(w, err)
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
		callbackhttp.WriteAdmissionError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *Adapter) codecFor(ctx context.Context, binding channels.BindingSnapshot) (callbackCodec, error) {
	scope := binding.Scope()
	token, err := a.secrets.ResolveSecret(ctx, scope, binding.TokenRef)
	if err != nil {
		return callbackCodec{}, fmt.Errorf("resolve wecom token: %w", err)
	}
	// Binding has no provider-specific EncodingAESKeyRef. For the WeCom AI
	// Bot contract, SigningSecretRef is the configured EncodingAESKey ref;
	// Secret remains a separate provider secret and must not be guessed here.
	encodingAESKey, err := a.secrets.ResolveSecret(ctx, scope, binding.SigningSecretRef)
	if err != nil {
		return callbackCodec{}, fmt.Errorf("resolve wecom encoding aes key: %w", err)
	}
	return newCallbackCodec(token, encodingAESKey)
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
			return channels.ChannelInput{}, errAttachmentIngestorRequired
		}
		media := append([]channels.ProviderMediaRef(nil), envelope.Media...)
		input, err = a.attachmentIngestor.Prepare(ctx, input, media)
		if err != nil {
			return channels.ChannelInput{}, fmt.Errorf("prepare wecom media: %w", err)
		}
	}
	if envelope.responseURL != "" {
		input, err = channels.WithMessageReplyTarget(input, channels.MessageReplyTarget{
			ProviderTarget: envelope.responseURL,
			ExpiresAt:      a.now().UTC().Add(wecomMessageTargetTTL),
		})
		if err != nil {
			return channels.ChannelInput{}, err
		}
	}
	return input, nil
}

type signedCallbackQuery struct {
	signature string
	timestamp string
	nonce     string
	encrypted string
}

func signedQuery(r *http.Request, encrypted string) (signedCallbackQuery, error) {
	if r == nil || r.URL == nil || encrypted == "" {
		return signedCallbackQuery{}, errCallbackQuery
	}
	query, err := oneValue(r.URL.Query(), "msg_signature")
	if err != nil {
		return signedCallbackQuery{}, errCallbackQuery
	}
	timestamp, err := oneValue(r.URL.Query(), "timestamp")
	if err != nil {
		return signedCallbackQuery{}, errCallbackQuery
	}
	nonce, err := oneValue(r.URL.Query(), "nonce")
	if err != nil {
		return signedCallbackQuery{}, errCallbackQuery
	}
	return signedCallbackQuery{
		signature: query,
		timestamp: timestamp,
		nonce:     nonce,
		encrypted: encrypted,
	}, nil
}

func oneValue(values url.Values, key string) (string, error) {
	items, ok := values[key]
	if !ok || len(items) != 1 || items[0] == "" {
		return "", errCallbackQuery
	}
	return items[0], nil
}

func checkTimestamp(value string, now time.Time, skew time.Duration) error {
	timestamp, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return errCallbackTimestamp
	}
	difference := now.Unix() - timestamp
	if difference < 0 {
		difference = -difference
	}
	if difference > int64(skew/time.Second) {
		return errCallbackTimestamp
	}
	return nil
}

func decodeEncryptedCallback(body []byte) (string, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return "", errCallbackMessage
	}
	var encrypted string
	found := false
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return "", errCallbackMessage
		}
		name, ok := key.(string)
		if !ok || name != "encrypt" || found {
			return "", errCallbackMessage
		}
		if err := decoder.Decode(&encrypted); err != nil || encrypted == "" {
			return "", errCallbackMessage
		}
		found = true
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') || !found {
		return "", errCallbackMessage
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", errCallbackMessage
	}
	return encrypted, nil
}

type callbackMessage struct {
	MessageID   string          `json:"msgid"`
	AIBotID     string          `json:"aibotid"`
	ChatID      string          `json:"chatid"`
	ChatType    string          `json:"chattype"`
	From        callbackFrom    `json:"from"`
	ResponseURL string          `json:"response_url"`
	MessageType string          `json:"msgtype"`
	CreateTime  int64           `json:"create_time"`
	Text        callbackText    `json:"text"`
	Image       callbackMedia   `json:"image"`
	File        callbackMedia   `json:"file"`
	Mixed       callbackMixed   `json:"mixed"`
	Stream      callbackStream  `json:"stream"`
	Event       json.RawMessage `json:"event"`
}

type callbackFrom struct {
	UserID string `json:"userid"`
}

type callbackText struct {
	Content string `json:"content"`
}

type callbackMedia struct {
	URL string `json:"url"`
}

type callbackMixed struct {
	Items []callbackMixedItem `json:"msg_item"`
}

type callbackMixedItem struct {
	MessageType string        `json:"msgtype"`
	Text        callbackText  `json:"text"`
	Image       callbackMedia `json:"image"`
	File        callbackMedia `json:"file"`
}

type callbackStream struct {
	ID string `json:"id"`
}

func normalizeCallback(binding channels.BindingSnapshot, callback callbackMessage) (VerifiedProviderEnvelope, error) {
	if callback.AIBotID == "" || callback.AIBotID != binding.ExternalAccount {
		return VerifiedProviderEnvelope{}, errWeComBindingAccountMismatch
	}
	messageID, err := channels.NormalizeExternalID(callback.MessageID)
	if err != nil {
		return VerifiedProviderEnvelope{}, errWeComMessageIDRequired
	}
	senderID, err := channels.NormalizeExternalID(callback.From.UserID)
	if err != nil {
		return VerifiedProviderEnvelope{}, errWeComSenderRequired
	}
	conversationKind, chatID, err := normalizeConversation(callback.ChatType, callback.ChatID)
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	messageType, text, media, callbackContext, err := normalizeMessage(callback, conversationKind)
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	var providerTimestamp time.Time
	if callback.CreateTime > 0 {
		providerTimestamp = time.Unix(callback.CreateTime, 0).UTC()
	}
	responseURL := strings.TrimSpace(callback.ResponseURL)
	if responseURL != "" {
		if err := validateResponseURL(responseURL); err != nil {
			return VerifiedProviderEnvelope{}, errWeComResponseTargetInvalid
		}
	}
	mapping := channels.ChannelMappingInput{
		ExternalSenderID:     senderID,
		ProviderSenderTarget: senderID,
	}
	if conversationKind == channels.ConversationGroup {
		mapping.ExternalChatID = chatID
		mapping.ProviderConversationTarget = chatID
	}
	envelope := VerifiedProviderEnvelope{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		Channel:           binding.Channel,
		BindingID:         binding.BindingID,
		BindingRevision:   binding.BindingRevision,
		ExternalMessageID: messageID,
		SenderID:          senderID,
		ConversationKind:  conversationKind,
		ChatID:            chatID,
		MessageType:       messageType,
		Text:              text,
		Media:             media,
		ProviderTimestamp: providerTimestamp,
		Context:           callbackContext,
		mapping:           mapping,
		responseURL:       responseURL,
	}
	if err := envelope.Validate(); err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	return envelope, nil
}

func normalizeConversation(chatType, chatID string) (channels.ConversationKind, string, error) {
	switch chatType {
	case "single":
		if strings.TrimSpace(chatID) != "" {
			return "", "", errWeComConversationInvalid
		}
		return channels.ConversationDirect, "", nil
	case "group":
		normalizedChatID, err := channels.NormalizeExternalID(chatID)
		if err != nil {
			return "", "", errWeComConversationInvalid
		}
		return channels.ConversationGroup, normalizedChatID, nil
	default:
		return "", "", errWeComConversationInvalid
	}
}

func normalizeMessage(
	callback callbackMessage,
	conversationKind channels.ConversationKind,
) (channels.MessageType, string, []channels.ProviderMediaRef, ProviderCallbackContext, error) {
	var emptyContext ProviderCallbackContext
	if callback.MessageType == "image" || callback.MessageType == "file" {
		if conversationKind != channels.ConversationDirect {
			return "", "", nil, emptyContext, errWeComMessageConversation
		}
	}
	switch callback.MessageType {
	case "text":
		if callback.Text.Content == "" {
			return "", "", nil, emptyContext, errCallbackMessage
		}
		return channels.MessageTypeText, callback.Text.Content, nil, emptyContext, nil
	case "image":
		kind, text, media, err := singleMedia(channels.MessageTypeImage, callback.Image.URL)
		return kind, text, media, emptyContext, err
	case "file":
		kind, text, media, err := singleMedia(channels.MessageTypeFile, callback.File.URL)
		return kind, text, media, emptyContext, err
	case "mixed":
		kind, text, media, err := normalizeMixed(callback.Mixed.Items)
		return kind, text, media, emptyContext, err
	case "card":
		return channels.MessageTypeCard, "", nil, emptyContext, nil
	case "stream":
		streamID, err := channels.NormalizeExternalID(callback.Stream.ID)
		if err != nil {
			return "", "", nil, emptyContext, errCallbackMessage
		}
		return channels.MessageTypeEvent, "", nil, ProviderCallbackContext{StreamID: streamID}, nil
	case "event":
		eventType, eventPayload, err := normalizeEvent(callback.Event)
		if err != nil {
			return "", "", nil, emptyContext, err
		}
		return channels.MessageTypeEvent, "", nil, ProviderCallbackContext{
			EventType:    eventType,
			EventPayload: eventPayload,
		}, nil
	default:
		return channels.MessageTypeUnsupported, "", nil, emptyContext, nil
	}
}

func singleMedia(kind channels.MessageType, reference string) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	if err := validateMediaURL(reference); err != nil {
		return "", "", nil, errWeComUnsupportedMediaPayload
	}
	return kind, "", []channels.ProviderMediaRef{{Kind: kind, Reference: reference}}, nil
}

func normalizeMixed(items []callbackMixedItem) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	if len(items) == 0 {
		return "", "", nil, errCallbackMessage
	}
	var texts []string
	var media []channels.ProviderMediaRef
	for _, item := range items {
		switch item.MessageType {
		case "text":
			if item.Text.Content == "" {
				return "", "", nil, errCallbackMessage
			}
			texts = append(texts, item.Text.Content)
		case "image":
			if err := validateMediaURL(item.Image.URL); err != nil {
				return "", "", nil, errWeComUnsupportedMediaPayload
			}
			media = append(media, channels.ProviderMediaRef{Kind: channels.MessageTypeImage, Reference: item.Image.URL})
		case "file":
			if err := validateMediaURL(item.File.URL); err != nil {
				return "", "", nil, errWeComUnsupportedMediaPayload
			}
			media = append(media, channels.ProviderMediaRef{Kind: channels.MessageTypeFile, Reference: item.File.URL})
		default:
			return "", "", nil, errWeComUnsupportedMediaPayload
		}
	}
	text := strings.Join(texts, "\n")
	if len(media) == 0 {
		return channels.MessageTypeText, text, nil, nil
	}
	if text == "" && len(media) == 1 {
		return media[0].Kind, text, media, nil
	}
	return channels.MessageTypeMixed, text, media, nil
}

func normalizeEvent(raw json.RawMessage) (string, []byte, error) {
	if len(raw) == 0 {
		return "", nil, errCallbackMessage
	}
	var event struct {
		EventType string `json:"eventtype"`
	}
	if err := json.Unmarshal(raw, &event); err != nil {
		return "", nil, errCallbackMessage
	}
	eventType, err := channels.NormalizeExternalID(event.EventType)
	if err != nil {
		return "", nil, errCallbackMessage
	}
	return eventType, slices.Clone(raw), nil
}

func validateResponseURL(value string) error {
	if value == "" || len(value) > maxProviderURLLength {
		return errWeComResponseTargetInvalid
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.Port() != "" || !strings.EqualFold(parsed.Hostname(), "qyapi.weixin.qq.com") {
		return errWeComResponseTargetInvalid
	}
	return nil
}

func validateMediaURL(value string) error {
	if value == "" || len(value) > maxProviderURLLength {
		return errWeComResponseTargetInvalid
	}
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.Port() != "" {
		return errWeComResponseTargetInvalid
	}
	return nil
}
