package feishu

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	feishuMessageEventType  = "im.message.receive_v1"
	feishuRecallEventType   = "im.message.recalled_v1"
	feishuChallengeType     = "url_verification"
	feishuEventCallbackType = "event_callback"

	feishuTargetUser         = "open_id"
	feishuTargetConversation = "chat_id"
	feishuTargetTopic        = "thread_id"
	feishuTargetMessage      = "message_id"
)

var (
	errFeishuCallbackBody      = errors.New("feishu callback body is invalid")
	errFeishuCallbackSignature = errors.New("invalid feishu callback signature")
	errFeishuCallbackTimestamp = errors.New("invalid feishu callback timestamp")
	errFeishuCallbackToken     = errors.New("invalid feishu verification token")
	errFeishuCallbackEvent     = errors.New("feishu callback event is invalid")
	errFeishuCallbackType      = errors.New("feishu callback type is invalid")
	errFeishuBindingAccount    = errors.New("feishu binding account does not match callback")
	errFeishuMessageID         = errors.New("feishu message id is required")
	errFeishuSenderID          = errors.New("feishu sender open_id is required")
	errFeishuConversation      = errors.New("feishu conversation is invalid")
	errFeishuMessageContent    = errors.New("feishu message content is invalid")
	errFeishuUnsupportedTarget = errors.New("feishu provider target is invalid")
	errFeishuTargetMessage     = errors.New("feishu message reply target is invalid")
	errFeishuRecallEvent       = errors.New("feishu recall event is invalid")
	errFeishuRecallEventID     = errors.New("feishu recall event id is required")
)

// VerifiedProviderEnvelope is the provider-neutral result of Feishu callback
// verification. Provider JSON and provider secrets stop at this boundary.
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

	mapping     channels.ChannelMappingInput
	replyTarget string
}

// Validate checks the normalized fields needed to build a ChannelInput.
func (e VerifiedProviderEnvelope) Validate() error {
	if e.TenantID == "" || e.AppID == "" {
		return errors.New("verified feishu envelope scope is required")
	}
	if e.Channel != channels.ChannelFeishu {
		return errors.New("verified feishu envelope channel is invalid")
	}
	if e.BindingID == "" || e.BindingRevision <= 0 {
		return errors.New("verified feishu envelope binding is invalid")
	}
	normalizedMessageID, err := channels.NormalizeExternalID(e.ExternalMessageID)
	if err != nil || normalizedMessageID != e.ExternalMessageID {
		return errFeishuMessageID
	}
	normalizedSenderID, err := channels.NormalizeExternalID(e.SenderID)
	if err != nil || normalizedSenderID != e.SenderID {
		return errFeishuSenderID
	}
	if err := e.ConversationKind.Validate(); err != nil {
		return err
	}
	switch e.ConversationKind {
	case channels.ConversationDirect:
		if e.ChatID != "" || e.ThreadID != "" {
			return errFeishuConversation
		}
	case channels.ConversationGroup:
		if !isNormalizedID(e.ChatID) || e.ThreadID != "" {
			return errFeishuConversation
		}
	case channels.ConversationTopic:
		if !isNormalizedID(e.ChatID) || !isNormalizedID(e.ThreadID) {
			return errFeishuConversation
		}
	default:
		return errFeishuConversation
	}
	if err := e.MessageType.Validate(); err != nil {
		return err
	}
	if e.MessageType == channels.MessageTypeText && e.Text == "" {
		return errFeishuMessageContent
	}
	if (e.MessageType == channels.MessageTypeImage || e.MessageType == channels.MessageTypeFile) && len(e.Media) == 0 {
		return errFeishuMessageContent
	}
	if e.MessageType == channels.MessageTypeMixed && e.Text == "" && len(e.Media) == 0 {
		return errFeishuMessageContent
	}
	if !utf8.ValidString(e.Text) {
		return errFeishuMessageContent
	}
	for _, media := range e.Media {
		if err := media.Validate(); err != nil {
			return fmt.Errorf("feishu media: %w", err)
		}
	}
	if err := e.mapping.Validate(e.ConversationKind); err != nil {
		return fmt.Errorf("feishu mapping: %w", err)
	}
	if e.replyTarget == "" {
		return errFeishuTargetMessage
	}
	targetKind, targetID, err := parseProviderTarget(e.replyTarget)
	if err != nil || targetKind != feishuTargetMessage || targetID != e.ExternalMessageID {
		return errFeishuTargetMessage
	}
	return nil
}

type callbackEnvelope struct {
	Type      string                 `json:"type"`
	Token     string                 `json:"token"`
	Challenge string                 `json:"challenge"`
	Encrypt   string                 `json:"encrypt"`
	Header    *larkevent.EventHeader `json:"header"`
}

type callbackMetadata struct {
	RequestType string
	EventType   string
	EventID     string
	Token       string
	Challenge   string
	AppID       string
	TenantKey   string
}

func decodeCallback(
	body []byte,
	headers http.Header,
	encryptKey string,
	now time.Time,
	maxClockSkew time.Duration,
) ([]byte, callbackMetadata, error) {
	if len(body) == 0 {
		return nil, callbackMetadata{}, errFeishuCallbackBody
	}
	var outer callbackEnvelope
	if err := json.Unmarshal(body, &outer); err != nil {
		return nil, callbackMetadata{}, fmt.Errorf("%w: json", errFeishuCallbackBody)
	}
	plain := body
	if outer.Encrypt != "" {
		if encryptKey == "" {
			return nil, callbackMetadata{}, errFeishuCallbackSignature
		}
		var err error
		plain, err = larkevent.EventDecrypt(outer.Encrypt, encryptKey)
		if err != nil {
			return nil, callbackMetadata{}, fmt.Errorf("%w: decrypt", errFeishuCallbackBody)
		}
	}
	metadata, err := parseCallbackMetadata(plain)
	if err != nil {
		return nil, callbackMetadata{}, err
	}
	// The official SDK skips signature verification for URL challenges. Keep
	// that behavior, while requiring a fresh request timestamp for every
	// normal callback and a signature when an Encrypt Key is configured.
	if metadata.RequestType != feishuChallengeType {
		if err := verifyCallbackTimestamp(headers, now, maxClockSkew); err != nil {
			return nil, callbackMetadata{}, err
		}
		if encryptKey != "" {
			if err := verifyCallbackSignature(headers, encryptKey, body); err != nil {
				return nil, callbackMetadata{}, err
			}
		}
	}
	return plain, metadata, nil
}

func parseCallbackMetadata(body []byte) (callbackMetadata, error) {
	var envelope callbackEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return callbackMetadata{}, fmt.Errorf("%w: json", errFeishuCallbackBody)
	}
	metadata := callbackMetadata{
		RequestType: strings.TrimSpace(envelope.Type),
		Token:       envelope.Token,
		Challenge:   envelope.Challenge,
	}
	if envelope.Header != nil {
		metadata.EventID = envelope.Header.EventID
		metadata.EventType = envelope.Header.EventType
		metadata.AppID = envelope.Header.AppID
		metadata.TenantKey = envelope.Header.TenantKey
		if envelope.Header.Token != "" {
			metadata.Token = envelope.Header.Token
		}
	}
	if metadata.RequestType == "" && metadata.EventType != "" {
		metadata.RequestType = feishuEventCallbackType
	}
	if metadata.RequestType == "" {
		return callbackMetadata{}, errFeishuCallbackType
	}
	return metadata, nil
}

func verifyCallbackTimestamp(headers http.Header, now time.Time, maxClockSkew time.Duration) error {
	timestamp, err := oneHeaderValue(headers, larkevent.EventRequestTimestamp)
	if err != nil {
		return errFeishuCallbackTimestamp
	}
	seconds, err := strconv.ParseInt(strings.TrimSpace(timestamp), 10, 64)
	if err != nil || seconds <= 0 || now.IsZero() || maxClockSkew <= 0 {
		return errFeishuCallbackTimestamp
	}
	providerTime := time.Unix(seconds, 0)
	if providerTime.Before(now.Add(-maxClockSkew)) || providerTime.After(now.Add(maxClockSkew)) {
		return errFeishuCallbackTimestamp
	}
	return nil
}

func verifyCallbackSignature(headers http.Header, encryptKey string, body []byte) error {
	timestamp, err := oneHeaderValue(headers, larkevent.EventRequestTimestamp)
	if err != nil {
		return errFeishuCallbackSignature
	}
	nonce, err := oneHeaderValue(headers, larkevent.EventRequestNonce)
	if err != nil {
		return errFeishuCallbackSignature
	}
	signature, err := oneHeaderValue(headers, larkevent.EventSignature)
	if err != nil {
		return errFeishuCallbackSignature
	}
	expected := larkevent.Signature(timestamp, nonce, encryptKey, string(body))
	if len(signature) != len(expected) || subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) != 1 {
		return errFeishuCallbackSignature
	}
	return nil
}

func oneHeaderValue(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) != 1 || values[0] == "" {
		return "", errFeishuCallbackSignature
	}
	return values[0], nil
}

func verifyCallbackMetadata(metadata callbackMetadata, binding channels.BindingSnapshot, verificationToken string) error {
	if verificationToken == "" || metadata.Token == "" ||
		len(verificationToken) != len(metadata.Token) ||
		subtle.ConstantTimeCompare([]byte(verificationToken), []byte(metadata.Token)) != 1 {
		return errFeishuCallbackToken
	}
	if metadata.AppID != "" && metadata.AppID != binding.ExternalAccount {
		return errFeishuBindingAccount
	}
	if metadata.TenantKey != "" && metadata.TenantKey != binding.ExternalAccountScope {
		return errFeishuBindingAccount
	}
	if metadata.RequestType == feishuChallengeType {
		if metadata.Challenge == "" {
			return errFeishuCallbackType
		}
		return nil
	}
	if metadata.RequestType != feishuEventCallbackType || metadata.EventType == "" {
		return errFeishuCallbackType
	}
	if metadata.AppID == "" || metadata.AppID != binding.ExternalAccount ||
		metadata.TenantKey == "" || binding.ExternalAccountScope == "" ||
		metadata.TenantKey != binding.ExternalAccountScope {
		return errFeishuBindingAccount
	}
	return nil
}

func normalizeMessageEvent(binding channels.BindingSnapshot, received *larkim.P2MessageReceiveV1) (VerifiedProviderEnvelope, error) {
	if received == nil || received.EventV2Base == nil || received.EventV2Base.Header == nil || received.Event == nil || received.Event.Message == nil {
		return VerifiedProviderEnvelope{}, errFeishuCallbackEvent
	}
	header := received.EventV2Base.Header
	if header.AppID != binding.ExternalAccount ||
		header.TenantKey == "" || binding.ExternalAccountScope == "" ||
		header.TenantKey != binding.ExternalAccountScope {
		return VerifiedProviderEnvelope{}, errFeishuBindingAccount
	}
	message := received.Event.Message
	senderID := senderOpenID(received.Event.Sender)
	normalizedSenderID, err := channels.NormalizeExternalID(senderID)
	if err != nil {
		return VerifiedProviderEnvelope{}, errFeishuSenderID
	}
	messageID, err := channels.NormalizeExternalID(valueOf(message.MessageId))
	if err != nil {
		return VerifiedProviderEnvelope{}, errFeishuMessageID
	}
	conversationKind, chatID, threadID, err := normalizeConversation(
		valueOf(message.ChatType), valueOf(message.ChatId), valueOf(message.ThreadId),
	)
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	messageType, text, media, err := normalizeMessageContent(valueOf(message.MessageType), valueOf(message.Content))
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	providerTimestamp, err := providerTimestamp(valueOf(message.CreateTime))
	if err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	mapping := channels.ChannelMappingInput{
		ExternalSenderID:     normalizedSenderID,
		ProviderSenderTarget: providerTarget(feishuTargetUser, normalizedSenderID),
	}
	if conversationKind == channels.ConversationGroup || conversationKind == channels.ConversationTopic {
		mapping.ExternalChatID = chatID
		mapping.ProviderConversationTarget = providerTarget(feishuTargetConversation, chatID)
	}
	if conversationKind == channels.ConversationTopic {
		mapping.ExternalThreadID = threadID
		mapping.ProviderThreadTarget = providerTarget(feishuTargetTopic, threadID)
	}
	envelope := VerifiedProviderEnvelope{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		Channel:           channels.ChannelFeishu,
		BindingID:         binding.BindingID,
		BindingRevision:   binding.BindingRevision,
		ExternalMessageID: messageID,
		SenderID:          normalizedSenderID,
		ConversationKind:  conversationKind,
		ChatID:            chatID,
		ThreadID:          threadID,
		MessageType:       messageType,
		Text:              text,
		Media:             media,
		ProviderTimestamp: providerTimestamp,
		mapping:           mapping,
		replyTarget:       providerTarget(feishuTargetMessage, messageID),
	}
	if err := envelope.Validate(); err != nil {
		return VerifiedProviderEnvelope{}, err
	}
	return envelope, nil
}

// normalizeRecallEvent converts the official Feishu recall event into the
// provider-neutral recall boundary. The payload digest is calculated over the
// already verified/decrypted callback, while scope and binding authorization
// come only from the route snapshot.
func normalizeRecallEvent(
	binding channels.BindingSnapshot,
	metadata callbackMetadata,
	recalled *larkim.P2MessageRecalledV1,
	payloadHash []byte,
) (channels.RecallRequest, error) {
	if recalled == nil || recalled.EventV2Base == nil || recalled.EventV2Base.Header == nil || recalled.Event == nil {
		return channels.RecallRequest{}, errFeishuRecallEvent
	}
	header := recalled.EventV2Base.Header
	if header.EventType != feishuRecallEventType ||
		header.AppID != binding.ExternalAccount ||
		header.TenantKey == "" || binding.ExternalAccountScope == "" ||
		header.TenantKey != binding.ExternalAccountScope {
		return channels.RecallRequest{}, errFeishuBindingAccount
	}
	eventID, err := channels.NormalizeExternalID(metadata.EventID)
	if err != nil || eventID != metadata.EventID || eventID != header.EventID {
		return channels.RecallRequest{}, errFeishuRecallEventID
	}
	messageID, err := channels.NormalizeExternalID(valueOf(recalled.Event.MessageId))
	if err != nil {
		return channels.RecallRequest{}, errFeishuMessageID
	}
	if len(payloadHash) != sha256.Size {
		return channels.RecallRequest{}, errFeishuRecallEvent
	}
	request := channels.RecallRequest{
		TenantID:          binding.TenantID,
		AppID:             binding.AppID,
		BindingID:         binding.BindingID,
		Channel:           channels.ChannelFeishu,
		ExternalEventID:   eventID,
		ExternalMessageID: messageID,
		PayloadHash:       append([]byte(nil), payloadHash...),
	}
	if err := request.Validate(); err != nil {
		return channels.RecallRequest{}, fmt.Errorf("feishu recall: %w", err)
	}
	return request, nil
}

func senderOpenID(sender *larkim.EventSender) string {
	if sender == nil || sender.SenderId == nil || sender.SenderId.OpenId == nil {
		return ""
	}
	return *sender.SenderId.OpenId
}

func normalizeConversation(chatType, chatID, threadID string) (channels.ConversationKind, string, string, error) {
	switch chatType {
	case "p2p":
		if threadID != "" {
			return "", "", "", errFeishuConversation
		}
		// Feishu supplies a chat_id for some private events. Direct-message
		// mapping intentionally uses only the sender identity.
		return channels.ConversationDirect, "", "", nil
	case "group":
		normalizedChatID, err := channels.NormalizeExternalID(chatID)
		if err != nil {
			return "", "", "", errFeishuConversation
		}
		if threadID == "" {
			return channels.ConversationGroup, normalizedChatID, "", nil
		}
		normalizedThreadID, err := channels.NormalizeExternalID(threadID)
		if err != nil {
			return "", "", "", errFeishuConversation
		}
		return channels.ConversationTopic, normalizedChatID, normalizedThreadID, nil
	default:
		return "", "", "", errFeishuConversation
	}
}

func normalizeMessageContent(messageType, raw string) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	switch strings.ToLower(strings.TrimSpace(messageType)) {
	case "text":
		var content struct {
			Text string `json:"text"`
		}
		if err := unmarshalObject(raw, &content); err != nil || content.Text == "" || !utf8.ValidString(content.Text) {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeText, content.Text, nil, nil
	case "post":
		return normalizePost(raw)
	case "image":
		var content struct {
			ImageKey string `json:"image_key"`
		}
		if err := unmarshalObject(raw, &content); err != nil || !isNormalizedID(content.ImageKey) {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeImage, "", []channels.ProviderMediaRef{{
			Kind: channels.MessageTypeImage, Reference: content.ImageKey,
		}}, nil
	case "file":
		var content struct {
			FileKey string `json:"file_key"`
		}
		if err := unmarshalObject(raw, &content); err != nil || !isNormalizedID(content.FileKey) {
			return "", "", nil, errFeishuMessageContent
		}
		return channels.MessageTypeFile, "", []channels.ProviderMediaRef{{
			Kind: channels.MessageTypeFile, Reference: content.FileKey,
		}}, nil
	case "interactive":
		var content map[string]json.RawMessage
		if err := unmarshalObject(raw, &content); err != nil || len(content) == 0 {
			return "", "", nil, errFeishuMessageContent
		}
		// The current shared ChannelInput has no action-event contract. Do not
		// silently turn a provider card into an empty executable message; let
		// admission durably reject it as an unsupported inbound type.
		return channels.MessageTypeUnsupported, "", nil, nil
	default:
		// Verification succeeded, but the current platform contract does not
		// expose this provider content to Gateway or Runner.
		return channels.MessageTypeUnsupported, "", nil, nil
	}
}

type postLocale struct {
	Title   string          `json:"title"`
	Content [][]postElement `json:"content"`
}

type postElement struct {
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	UserName string `json:"user_name"`
	ImageKey string `json:"image_key"`
}

func normalizePost(raw string) (channels.MessageType, string, []channels.ProviderMediaRef, error) {
	var locales map[string]postLocale
	if err := unmarshalObject(raw, &locales); err != nil || len(locales) == 0 {
		return "", "", nil, errFeishuMessageContent
	}
	localeNames := make([]string, 0, len(locales))
	for name := range locales {
		localeNames = append(localeNames, name)
	}
	sort.Strings(localeNames)
	selected := localeNames[0]
	for _, preferred := range []string{"zh_cn", "en_us"} {
		if _, ok := locales[preferred]; ok {
			selected = preferred
			break
		}
	}
	locale := locales[selected]
	var textParts []string
	if locale.Title != "" {
		textParts = append(textParts, locale.Title)
	}
	var media []channels.ProviderMediaRef
	for _, line := range locale.Content {
		for _, element := range line {
			switch element.Tag {
			case "img":
				if !isNormalizedID(element.ImageKey) {
					return "", "", nil, errFeishuMessageContent
				}
				media = append(media, channels.ProviderMediaRef{
					Kind: channels.MessageTypeImage, Reference: element.ImageKey,
				})
			case "text", "a", "at", "emoji":
				value := element.Text
				if value == "" {
					value = element.UserName
				}
				if value != "" {
					textParts = append(textParts, value)
				}
			}
		}
	}
	text := strings.Join(textParts, "\n")
	if text == "" && len(media) == 0 {
		return "", "", nil, errFeishuMessageContent
	}
	if len(media) == 0 {
		return channels.MessageTypeText, text, nil, nil
	}
	if text == "" && len(media) == 1 {
		return channels.MessageTypeImage, text, media, nil
	}
	return channels.MessageTypeMixed, text, media, nil
}

func unmarshalObject(raw string, target any) error {
	if strings.TrimSpace(raw) == "" {
		return errFeishuMessageContent
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	var object map[string]json.RawMessage
	if err := decoder.Decode(&object); err != nil || object == nil {
		return errFeishuMessageContent
	}
	if err := decoder.Decode(&struct{}{}); err == nil {
		return errFeishuMessageContent
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return errFeishuMessageContent
	}
	return nil
}

func providerTimestamp(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	millis, err := strconv.ParseInt(value, 10, 64)
	if err != nil || millis < 0 {
		return time.Time{}, errFeishuCallbackEvent
	}
	if millis == 0 {
		return time.Time{}, nil
	}
	return time.UnixMilli(millis).UTC(), nil
}

func providerTarget(kind, id string) string {
	return kind + ":" + id
}

func parseProviderTarget(value string) (string, string, error) {
	kind, id, ok := strings.Cut(value, ":")
	if !ok || kind == "" || id == "" || strings.Contains(id, ":") {
		return "", "", errFeishuUnsupportedTarget
	}
	switch kind {
	case feishuTargetUser, feishuTargetConversation, feishuTargetTopic, feishuTargetMessage:
	default:
		return "", "", errFeishuUnsupportedTarget
	}
	normalized, err := channels.NormalizeExternalID(id)
	if err != nil || normalized != id || !utf8.ValidString(id) {
		return "", "", errFeishuUnsupportedTarget
	}
	return kind, id, nil
}

func isNormalizedID(value string) bool {
	normalized, err := channels.NormalizeExternalID(value)
	return err == nil && normalized == value
}

func valueOf(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
