package wecom

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	testToken       = "wecom-token"
	testRoute       = "r_test-route"
	testBindingID   = "binding-wecom"
	testExternalBot = "aibot-test"
)

var testNow = time.Unix(1_735_689_600, 0).UTC()

func TestAdapterURLVerification(t *testing.T) {
	adapter, codec, _, _ := newTestAdapter(t)
	encrypted, err := codec.encrypt([]byte("challenge"))
	if err != nil {
		t.Fatalf("encrypt challenge: %v", err)
	}
	request := signedRequest(t, http.MethodGet, "/im/wecom/"+testRoute, encrypted, "echostr")
	query, err := signedQuery(request, encrypted)
	if err != nil {
		t.Fatalf("parse verification query: %v", err)
	}
	if err := codec.verifySignature(query.timestamp, query.nonce, query.encrypted, query.signature); err != nil {
		t.Fatalf("verify challenge signature: %v, query=%#v", err, query)
	}
	if _, err := codec.decrypt(encrypted); err != nil {
		t.Fatalf("decrypt challenge: %v", err)
	}
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("verification status = %d, want %d, body=%q", response.Code, http.StatusOK, response.Body.String())
	}
	if response.Body.String() != "challenge" {
		t.Fatalf("verification body = %q", response.Body.String())
	}
	if strings.HasSuffix(response.Body.String(), "\n") {
		t.Fatal("verification response contains a trailing newline")
	}
}

func TestAdapterRejectsInvalidSignatureBeforeGateway(t *testing.T) {
	adapter, codec, admitter, _ := newTestAdapter(t)
	body := validCallbackJSON(t, "msg-1", "text", "hello")
	encrypted, err := codec.encrypt(body)
	if err != nil {
		t.Fatalf("encrypt callback: %v", err)
	}
	request := signedRequest(t, http.MethodPost, "/im/wecom/"+testRoute, encrypted, "body")
	request.URL.RawQuery = strings.Replace(request.URL.RawQuery, "msg_signature=", "msg_signature=invalid", 1)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("invalid signature status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if len(admitter.requests()) != 0 {
		t.Fatal("invalid signature reached Gateway")
	}
}

func TestAdapterSubmitsDirectMessageWithBindingScope(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	body := validCallbackJSON(t, "msg-direct", "text", "hello")
	request := callbackRequest(t, body)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("callback status = %d, want %d", response.Code, http.StatusOK)
	}
	requests := admitter.requests()
	if len(requests) != 1 {
		t.Fatalf("Gateway request count = %d, want 1", len(requests))
	}
	requestValue := requests[0]
	if requestValue.Identity.Tenant.TenantID != "tenant-a" || requestValue.Identity.Tenant.AppID != "app-a" {
		t.Fatalf("trusted scope = %#v", requestValue.Identity.Tenant)
	}
	if requestValue.Identity.Source != gateway.TenantSourceVerifiedChannelBinding ||
		requestValue.Identity.SourceID != testBindingID {
		t.Fatalf("trusted source = %#v", requestValue.Identity)
	}
	if requestValue.IdempotencyKey != "msg-direct" || requestValue.ChannelInput == nil {
		t.Fatalf("Gateway request = %#v", requestValue)
	}
	input := requestValue.ChannelInput
	if input.TenantID != "tenant-a" || input.AppID != "app-a" || input.BindingID != testBindingID {
		t.Fatalf("channel input scope = %#v", *input)
	}
	if input.Conversation.Kind != channels.ConversationDirect || input.Text != "hello" {
		t.Fatalf("channel input = %#v", *input)
	}
	mapping, ok := input.MappingInput()
	if !ok || mapping.ExternalSenderID != "user-1" || mapping.ProviderSenderTarget != "user-1" {
		t.Fatalf("direct mapping = %#v, present=%v", mapping, ok)
	}
	if strings.Contains(requestValue.Message.Text, "response_url") || strings.Contains(requestValue.Message.Text, "aibotid") {
		t.Fatal("provider fields leaked into Gateway message")
	}
}

func TestAdapterSubmitsGroupMessageWithGroupMapping(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	body := []byte(`{"msgid":"msg-group","aibotid":"aibot-test","chatid":"chat-1","chattype":"group","from":{"userid":"user-2"},"response_url":"https://example.test/reply","msgtype":"text","text":{"content":"group hello"}}`)
	request := callbackRequest(t, body)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("callback status = %d, want %d", response.Code, http.StatusOK)
	}
	requests := admitter.requests()
	if len(requests) != 1 || requests[0].ChannelInput == nil {
		t.Fatalf("Gateway requests = %#v", requests)
	}
	input := requests[0].ChannelInput
	if input.Conversation.Kind != channels.ConversationGroup {
		t.Fatalf("conversation kind = %q", input.Conversation.Kind)
	}
	mapping, ok := input.MappingInput()
	if !ok || mapping.ExternalChatID != "chat-1" || mapping.ProviderConversationTarget != "chat-1" {
		t.Fatalf("group mapping = %#v, present=%v", mapping, ok)
	}
	if mapping.ExternalSenderID != "user-2" {
		t.Fatalf("group sender = %q", mapping.ExternalSenderID)
	}
}

func TestAdapterRejectsBindingAccountMismatch(t *testing.T) {
	adapter, codec, admitter, _ := newTestAdapter(t)
	body := []byte(`{"msgid":"msg-1","aibotid":"another-bot","chattype":"single","from":{"userid":"user-1"},"msgtype":"text","text":{"content":"hello"}}`)
	encrypted, err := codec.encrypt(body)
	if err != nil {
		t.Fatalf("encrypt callback: %v", err)
	}
	request := signedRequest(t, http.MethodPost, "/im/wecom/"+testRoute, encrypted, "body")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("account mismatch status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterRejectsMissingMessageID(t *testing.T) {
	adapter, codec, admitter, _ := newTestAdapter(t)
	body := []byte(`{"aibotid":"aibot-test","chattype":"single","from":{"userid":"user-1"},"msgtype":"text","text":{"content":"hello"}}`)
	encrypted, err := codec.encrypt(body)
	if err != nil {
		t.Fatalf("encrypt callback: %v", err)
	}
	request := signedRequest(t, http.MethodPost, "/im/wecom/"+testRoute, encrypted, "body")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("missing id status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterRequiresAttachmentIngestorForMedia(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	body := []byte(`{"msgid":"msg-image","aibotid":"aibot-test","chattype":"single","from":{"userid":"user-1"},"msgtype":"image","image":{"url":"https://example.test/image"}}`)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, callbackRequest(t, body))
	if response.Code != http.StatusServiceUnavailable || len(admitter.requests()) != 0 {
		t.Fatalf("media without ingestor status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterPassesMediaThroughAttachmentBoundary(t *testing.T) {
	ingestor := &testAttachmentIngestor{}
	adapter, _, admitter, _ := newTestAdapter(t, WithAttachmentIngestor(ingestor))
	body := []byte(`{"msgid":"msg-image","aibotid":"aibot-test","chattype":"single","from":{"userid":"user-1"},"msgtype":"image","image":{"url":"https://example.test/image"}}`)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, callbackRequest(t, body))
	if response.Code != http.StatusOK {
		t.Fatalf("media callback status = %d, want %d", response.Code, http.StatusOK)
	}
	if len(ingestor.media) != 1 || ingestor.media[0].Kind != channels.MessageTypeImage {
		t.Fatalf("ingestor media = %#v", ingestor.media)
	}
	requests := admitter.requests()
	if len(requests) != 1 || requests[0].ChannelInput == nil || len(requests[0].ChannelInput.ArtifactRefs) != 1 {
		t.Fatalf("media Gateway request = %#v", requests)
	}
}

func TestAdapterRejectsOversizedCallback(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t, WithMaxCallbackBytes(32))
	body := []byte(`{"encrypt":"this body is larger than the configured limit"}`)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/im/wecom/"+testRoute, strings.NewReader(string(body)))
	request.Header.Set("Content-Type", "application/json")
	request.URL.RawQuery = signedQueryForTest("not-used")
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge || len(admitter.requests()) != 0 {
		t.Fatalf("oversized callback status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterDoesNotAckGatewayFailure(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	admitter.err = errors.New("database unavailable")
	body := validCallbackJSON(t, "msg-1", "text", "hello")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, callbackRequest(t, body))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("Gateway failure status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
}

func TestAdapterRequiresJSONContentType(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	body := validCallbackJSON(t, "msg-1", "text", "hello")
	request := callbackRequest(t, body)
	request.Header.Set("Content-Type", "text/plain")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("content type status=%d requests=%d", response.Code, len(admitter.requests()))
	}
	request = callbackRequest(t, body)
	request.Header.Del("Content-Type")
	response = httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("missing content type status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterRejectsMalformedEncryptedEnvelope(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	request := httptest.NewRequest(http.MethodPost, "/im/wecom/"+testRoute, strings.NewReader(`{"encrypt":"bad","extra":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.URL.RawQuery = signedQueryForTest("bad")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("malformed envelope status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterRejectsValidSignatureWithDecryptionFailure(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	request := signedRequest(t, http.MethodPost, "/im/wecom/"+testRoute, "not-valid-ciphertext", "encrypt")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("decryption failure status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterRejectsMissingSignature(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	request := callbackRequest(t, validCallbackJSON(t, "msg-1", "text", "hello"))
	query := request.URL.Query()
	query.Del("msg_signature")
	request.URL.RawQuery = query.Encode()
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || len(admitter.requests()) != 0 {
		t.Fatalf("missing signature status=%d requests=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterRejectsEmptyAndInvalidJSONBodies(t *testing.T) {
	adapter, _, admitter, _ := newTestAdapter(t)
	for _, body := range []string{"", "not-json"} {
		request := httptest.NewRequest(http.MethodPost, "/im/wecom/"+testRoute, strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		request.URL.RawQuery = signedQueryForTest("invalid")
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body %q status=%d, want %d", body, response.Code, http.StatusBadRequest)
		}
	}
	if len(admitter.requests()) != 0 {
		t.Fatal("invalid body reached Gateway")
	}
}

func TestAdapterRejectsDuplicateOrCaseVariantEncryptField(t *testing.T) {
	adapter, codec, admitter, _ := newTestAdapter(t)
	encrypted, err := codec.encrypt(validCallbackJSON(t, "msg-1", "text", "hello"))
	if err != nil {
		t.Fatalf("encrypt callback: %v", err)
	}
	for _, body := range []string{
		fmt.Sprintf(`{"encrypt":%q,"encrypt":%q}`, encrypted, encrypted),
		fmt.Sprintf(`{"Encrypt":%q}`, encrypted),
		fmt.Sprintf(`{"encrypt":%q}{}`, encrypted),
	} {
		request := signedRequest(t, http.MethodPost, "/im/wecom/"+testRoute, encrypted, "encrypt")
		request.Body = io.NopCloser(strings.NewReader(body))
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("body %q status=%d, want %d", body, response.Code, http.StatusBadRequest)
		}
	}
	if len(admitter.requests()) != 0 {
		t.Fatal("malformed encrypt envelope reached Gateway")
	}
}

func TestAdapterRejectsStaleAndFutureCallbacks(t *testing.T) {
	adapter, codec, admitter, _ := newTestAdapter(t)
	body := validCallbackJSON(t, "msg-1", "text", "hello")
	encrypted, err := codec.encrypt(body)
	if err != nil {
		t.Fatalf("encrypt callback: %v", err)
	}
	for _, timestamp := range []int64{testNow.Unix() - int64(defaultClockSkew/time.Second) - 1, testNow.Unix() + int64(defaultClockSkew/time.Second) + 1} {
		request := signedRequestWithTimestamp(t, http.MethodPost, "/im/wecom/"+testRoute, encrypted, timestamp)
		response := httptest.NewRecorder()
		adapter.ServeHTTP(response, request)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("timestamp %d status=%d, want %d", timestamp, response.Code, http.StatusBadRequest)
		}
	}
	if len(admitter.requests()) != 0 {
		t.Fatal("stale or future callback reached Gateway")
	}
}

func TestNormalizeCallbackPreservesStreamAndEventContext(t *testing.T) {
	binding := testBindingSnapshot()
	stream, err := normalizeCallback(binding, callbackMessage{
		MessageID:   "stream-1",
		AIBotID:     testExternalBot,
		ChatType:    "group",
		ChatID:      "chat-1",
		From:        callbackFrom{UserID: "user-1"},
		MessageType: "stream",
		Stream:      callbackStream{ID: "stream-id"},
	})
	if err != nil {
		t.Fatalf("normalize stream callback: %v", err)
	}
	if stream.MessageType != channels.MessageTypeEvent || stream.Context.StreamID != "stream-id" {
		t.Fatalf("stream envelope = %#v", stream)
	}
	event, err := normalizeCallback(binding, callbackMessage{
		MessageID:   "event-1",
		AIBotID:     testExternalBot,
		ChatType:    "single",
		From:        callbackFrom{UserID: "user-1"},
		MessageType: "event",
		Event:       json.RawMessage(`{"eventtype":"template_card_event","template_card_event":{"event_key":"ok"}}`),
	})
	if err != nil {
		t.Fatalf("normalize event callback: %v", err)
	}
	if event.Context.EventType != "template_card_event" || !bytes.Equal(event.Context.EventPayload, eventPayload(t)) {
		t.Fatalf("event envelope = %#v", event)
	}
}

func TestNormalizeCallbackRejectsUnsupportedConversationMessage(t *testing.T) {
	_, err := normalizeCallback(testBindingSnapshot(), callbackMessage{
		MessageID:   "image-group",
		AIBotID:     testExternalBot,
		ChatType:    "group",
		ChatID:      "chat-1",
		From:        callbackFrom{UserID: "user-1"},
		MessageType: "image",
		Image:       callbackMedia{URL: "https://example.test/image"},
	})
	if !errors.Is(err, errWeComMessageConversation) {
		t.Fatalf("normalize invalid conversation error=%v, want conversation mismatch", err)
	}
}

func TestNormalizeCallbackSupportsDirectMediaAndGroupMixed(t *testing.T) {
	binding := testBindingSnapshot()
	tests := []struct {
		name           string
		callback       callbackMessage
		wantType       channels.MessageType
		wantText       string
		wantMediaCount int
	}{
		{
			name: "direct file",
			callback: callbackMessage{
				MessageID: "file-1", AIBotID: testExternalBot, ChatType: "single",
				From: callbackFrom{UserID: "user-1"}, MessageType: "file",
				File: callbackMedia{URL: "https://example.test/file"},
			},
			wantType: channels.MessageTypeFile, wantMediaCount: 1,
		},
		{
			name: "direct voice",
			callback: callbackMessage{
				MessageID: "voice-1", AIBotID: testExternalBot, ChatType: "single",
				From: callbackFrom{UserID: "user-1"}, MessageType: "voice",
				Voice: callbackText{Content: "transcribed voice"},
			},
			wantType: channels.MessageTypeText, wantText: "transcribed voice",
		},
		{
			name: "group mixed",
			callback: callbackMessage{
				MessageID: "mixed-1", AIBotID: testExternalBot, ChatType: "group", ChatID: "chat-1",
				From: callbackFrom{UserID: "user-1"}, MessageType: "mixed",
				Mixed: callbackMixed{Items: []callbackMixedItem{
					{MessageType: "text", Text: callbackText{Content: "hello"}},
					{MessageType: "image", Image: callbackMedia{URL: "https://example.test/image"}},
				}},
			},
			wantType: channels.MessageTypeMixed, wantText: "hello", wantMediaCount: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envelope, err := normalizeCallback(binding, test.callback)
			if err != nil {
				t.Fatalf("normalize callback: %v", err)
			}
			if envelope.MessageType != test.wantType || envelope.Text != test.wantText || len(envelope.Media) != test.wantMediaCount {
				t.Fatalf("envelope = %#v", envelope)
			}
		})
	}
}

func TestOutboundClientUsesOneActiveResponseCall(t *testing.T) {
	var received []byte
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("request = %s %s", r.Method, r.Header.Get("Content-Type"))
		}
		received, _ = io.ReadAll(r.Body)
		_, _ = w.Write([]byte(`{"errcode":0}`))
	}))
	defer server.Close()
	reply := testReply(channels.ReplyOperationSend, channels.ReplyKindText)
	receipt, err := NewOutboundClient(server.Client()).SendOnce(context.Background(), reply, server.URL, channels.OutboundContext{})
	if err != nil {
		t.Fatalf("send reply: %v", err)
	}
	if receipt.ProviderMessageID != reply.ReplyID {
		t.Fatalf("receipt id=%q, want local reply id %q", receipt.ProviderMessageID, reply.ReplyID)
	}
	var payload struct {
		MessageType string `json:"msgtype"`
		Markdown    struct {
			Content string `json:"content"`
		} `json:"markdown"`
	}
	if err := json.Unmarshal(received, &payload); err != nil {
		t.Fatalf("decode outbound payload: %v", err)
	}
	if payload.MessageType != "markdown" || payload.Markdown.Content != reply.Text {
		t.Fatalf("outbound payload = %#v", payload)
	}
}

func TestOutboundClientDoesNotSendPassiveStreamUpdates(t *testing.T) {
	var calls int
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := NewOutboundClient(server.Client())
	for _, operation := range []channels.ReplyOperation{
		channels.ReplyOperationUpdate,
		channels.ReplyOperationFinalize,
	} {
		_, err := client.SendOnce(
			context.Background(),
			testReply(operation, channels.ReplyKindText),
			server.URL,
			channels.OutboundContext{StreamContext: "stream-1"},
		)
		if !errors.Is(err, errWeComPassiveReplyRequired) {
			t.Fatalf("operation %s error=%v, want passive response error", operation, err)
		}
	}
	if calls != 0 {
		t.Fatalf("active response_url calls=%d, want 0", calls)
	}
}

func TestPassiveReplyWriterWritesEncryptedCurrentCallbackResponse(t *testing.T) {
	writer, err := NewPassiveReplyWriter(
		testToken,
		base64Raw("01234567890123456789012345678901"),
		WithPassiveReplyClock(func() time.Time { return testNow }),
	)
	if err != nil {
		t.Fatalf("new passive reply writer: %v", err)
	}
	response := httptest.NewRecorder()
	receipt, err := writer.Write(
		response,
		testReply(channels.ReplyOperationUpdate, channels.ReplyKindText),
		"nonce-1",
		channels.OutboundContext{StreamContext: "stream-1"},
	)
	if err != nil {
		t.Fatalf("write passive reply: %v", err)
	}
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("passive response status=%d content-type=%q", response.Code, response.Header().Get("Content-Type"))
	}
	if receipt.ProviderMessageID != "reply-1" {
		t.Fatalf("receipt id=%q, want local reply id", receipt.ProviderMessageID)
	}
	var envelope struct {
		Encrypt      string `json:"encrypt"`
		MsgSignature string `json:"msgsignature"`
		Timestamp    int64  `json:"timestamp"`
		Nonce        string `json:"nonce"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode passive envelope: %v", err)
	}
	if envelope.Nonce != "nonce-1" || envelope.Timestamp != testNow.Unix() || envelope.Encrypt == "" {
		t.Fatalf("passive envelope = %#v", envelope)
	}
	codec, err := newCallbackCodec(testToken, base64Raw("01234567890123456789012345678901"))
	if err != nil {
		t.Fatalf("new passive verification codec: %v", err)
	}
	if err := codec.verifySignature(strconv.FormatInt(envelope.Timestamp, 10), envelope.Nonce, envelope.Encrypt, envelope.MsgSignature); err != nil {
		t.Fatalf("verify passive envelope: %v", err)
	}
	decrypted, err := codec.decrypt(envelope.Encrypt)
	if err != nil {
		t.Fatalf("decrypt passive payload: %v", err)
	}
	var payload struct {
		MessageType string `json:"msgtype"`
		Stream      struct {
			ID     string `json:"id"`
			Finish bool   `json:"finish"`
		} `json:"stream"`
	}
	if err := json.Unmarshal(decrypted, &payload); err != nil {
		t.Fatalf("decode stream payload: %v", err)
	}
	if payload.MessageType != "stream" || payload.Stream.ID != "stream-1" || payload.Stream.Finish {
		t.Fatalf("stream payload = %#v", payload)
	}
}

func TestPassiveReplyWriterRejectsOrdinarySend(t *testing.T) {
	writer, err := NewPassiveReplyWriter(testToken, base64Raw("01234567890123456789012345678901"))
	if err != nil {
		t.Fatalf("new passive reply writer: %v", err)
	}
	response := httptest.NewRecorder()
	_, err = writer.Write(
		response,
		testReply(channels.ReplyOperationSend, channels.ReplyKindText),
		"nonce-1",
		channels.OutboundContext{},
	)
	if !errors.Is(err, errWeComPassiveReplyRequired) {
		t.Fatalf("ordinary send error=%v, want passive response error", err)
	}
	if response.Code != http.StatusOK || response.Body.Len() != 0 {
		t.Fatalf("ordinary send response status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestCallbackCodecDecryptsIndependentProtocolVector(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	codec, err := newCallbackCodec(testToken, base64Raw(string(key)))
	if err != nil {
		t.Fatalf("new codec: %v", err)
	}
	message := []byte(`{"msgid":"fixed-vector"}`)
	encrypted := independentEncryptedMessage(t, key, bytes.Repeat([]byte{0x42}, aes.BlockSize), message, "")
	decrypted, err := codec.decrypt(encrypted)
	if err != nil {
		t.Fatalf("decrypt independent vector: %v", err)
	}
	if !bytes.Equal(decrypted, message) {
		t.Fatalf("decrypted vector = %q, want %q", decrypted, message)
	}
	withReceiveID := independentEncryptedMessage(t, key, bytes.Repeat([]byte{0x42}, aes.BlockSize), message, "unexpected")
	if _, err := codec.decrypt(withReceiveID); err == nil {
		t.Fatal("non-empty receive_id passed internal AI Bot decryption")
	}
}

func TestAdapterRejectsUnknownAndInactiveRoutes(t *testing.T) {
	adapter, _, _, resolver := newTestAdapter(t)
	unknown := httptest.NewRequest(http.MethodPost, "/im/wecom/r_missing", strings.NewReader(`{}`))
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, unknown)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown route status = %d, want %d", response.Code, http.StatusNotFound)
	}
	resolver.snapshot.Status = channels.BindingSuspended
	response = httptest.NewRecorder()
	adapter.ServeHTTP(response, callbackRequest(t, validCallbackJSON(t, "msg-1", "text", "hello")))
	if response.Code != http.StatusForbidden {
		t.Fatalf("inactive route status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

func TestCallbackCodecRejectsTamperedCiphertext(t *testing.T) {
	_, codec, _, _ := newTestAdapter(t)
	encrypted, err := codec.encrypt([]byte("payload"))
	if err != nil {
		t.Fatalf("encrypt payload: %v", err)
	}
	last := encrypted[len(encrypted)-1]
	replacement := byte('A')
	if last == replacement {
		replacement = 'B'
	}
	tampered := encrypted[:len(encrypted)-1] + string(replacement)
	if _, err := codec.decrypt(tampered); err == nil {
		t.Fatal("tampered ciphertext decrypted successfully")
	}
}

func TestCheckTimestampDoesNotOverflow(t *testing.T) {
	if err := checkTimestamp(strconv.FormatInt(int64(^uint64(0)>>1), 10), testNow, defaultClockSkew); err == nil {
		t.Fatal("extreme timestamp passed freshness check")
	}
}

type testRouteResolver struct {
	snapshot channels.BindingSnapshot
}

func (r *testRouteResolver) ResolveBindingByPublicRoute(
	_ context.Context,
	channel channels.Channel,
	publicRouteID string,
) (channels.BindingSnapshot, error) {
	if publicRouteID != r.snapshot.PublicRouteID {
		return channels.BindingSnapshot{}, channels.ErrBindingNotFound
	}
	if channel != r.snapshot.Channel {
		return channels.BindingSnapshot{}, channels.ErrBindingChannelMismatch
	}
	return r.snapshot, nil
}

type testSecretProvider struct {
	values map[string]string
}

func (p testSecretProvider) ResolveSecret(_ context.Context, _ tenant.Scope, ref tenant.SecretRef) (string, error) {
	value, ok := p.values[ref.Name]
	if !ok {
		return "", fmt.Errorf("secret %q not found", ref.Name)
	}
	return value, nil
}

type testAdmitter struct {
	mu       sync.Mutex
	recorded []gateway.AdmissionRequest
	err      error
}

func (a *testAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.mu.Lock()
	a.recorded = append(a.recorded, request)
	err := a.err
	a.mu.Unlock()
	if err != nil {
		return gateway.AdmissionResult{}, err
	}
	return gateway.AdmissionResult{RequestID: request.RequestID, ConfigVersion: "v1", TurnSeq: 1}, nil
}

func (a *testAdmitter) requests() []gateway.AdmissionRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]gateway.AdmissionRequest(nil), a.recorded...)
}

type testAttachmentIngestor struct {
	media []channels.ProviderMediaRef
}

type fakeOutboundClient struct {
	receipt channels.ProviderReceipt
	err     error
	calls   int
}

func (c *fakeOutboundClient) SendOnce(
	_ context.Context,
	_ channels.Reply,
	_ string,
	_ channels.OutboundContext,
) (channels.ProviderReceipt, error) {
	c.calls++
	if c.err != nil {
		return channels.ProviderReceipt{}, c.err
	}
	return c.receipt, nil
}

func TestFakeOutboundClientIsSingleOperationOnly(t *testing.T) {
	fake := &fakeOutboundClient{receipt: channels.ProviderReceipt{ProviderMessageID: "provider-1"}}
	var client channels.ProviderOutboundClient = fake
	receipt, err := client.SendOnce(context.Background(), testReply(channels.ReplyOperationSend, channels.ReplyKindText), "https://example.test/reply", channels.OutboundContext{})
	if err != nil || receipt.ProviderMessageID != "provider-1" || fake.calls != 1 {
		t.Fatalf("fake outbound result=%#v err=%v calls=%d", receipt, err, fake.calls)
	}
}

func TestFakeOutboundClientPropagatesProviderError(t *testing.T) {
	want := &ProviderSendError{StatusCode: http.StatusTooManyRequests, Code: 45009, Retryable: true}
	fake := &fakeOutboundClient{err: want}
	var client channels.ProviderOutboundClient = fake
	_, err := client.SendOnce(context.Background(), testReply(channels.ReplyOperationSend, channels.ReplyKindText), "https://example.test/reply", channels.OutboundContext{})
	var got *ProviderSendError
	if !errors.As(err, &got) || got != want || !got.Retryable || got.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("provider error=%v, want %#v", err, want)
	}
	if fake.calls != 1 {
		t.Fatalf("fake calls=%d, want 1", fake.calls)
	}
}

func (i *testAttachmentIngestor) Prepare(_ context.Context, input channels.ChannelInput, media []channels.ProviderMediaRef) (channels.ChannelInput, error) {
	i.media = append([]channels.ProviderMediaRef(nil), media...)
	input.ArtifactRefs = []string{"artifact://test/image"}
	return input, nil
}

func newTestAdapter(t *testing.T, opts ...AdapterOption) (*Adapter, callbackCodec, *testAdmitter, *testRouteResolver) {
	t.Helper()
	key := "01234567890123456789012345678901"
	resolver := &testRouteResolver{snapshot: channels.BindingSnapshot{Binding: channels.Binding{
		TenantID:         "tenant-a",
		AppID:            "app-a",
		BindingID:        testBindingID,
		Channel:          channels.ChannelWeCom,
		ExternalAccount:  testExternalBot,
		WebhookURL:       "https://example.test/im",
		TokenRef:         tenant.SecretRef{Name: "token"},
		SigningSecretRef: tenant.SecretRef{Name: "encoding-aes-key"},
		Secret:           tenant.SecretRef{Name: "unrelated-provider-secret"},
		PublicRouteID:    testRoute,
		BindingRevision:  3,
		Status:           channels.BindingActive,
	}}}
	secrets := testSecretProvider{values: map[string]string{
		"token":            testToken,
		"encoding-aes-key": base64Raw(key),
	}}
	admitter := &testAdmitter{}
	admissionGateway := gateway.New(gateway.WithAdmitter(admitter))
	adapter, err := NewAdapter(resolver, admissionGateway, platformsecret.SecretProvider(secrets), append(opts, WithClock(func() time.Time { return testNow }))...)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	codec, err := newCallbackCodec(testToken, base64Raw(key))
	if err != nil {
		t.Fatalf("new callback codec: %v", err)
	}
	return adapter, codec, admitter, resolver
}

func validCallbackJSON(t *testing.T, messageID, messageType, text string) []byte {
	t.Helper()
	value := callbackMessage{
		MessageID:   messageID,
		AIBotID:     testExternalBot,
		ChatType:    "single",
		From:        callbackFrom{UserID: "user-1"},
		MessageType: messageType,
		Text:        callbackText{Content: text},
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("marshal callback: %v", err)
	}
	return encoded
}

func callbackRequest(t *testing.T, body []byte) *http.Request {
	t.Helper()
	_, codec, _, _ := newTestAdapter(t)
	encrypted, err := codec.encrypt(body)
	if err != nil {
		t.Fatalf("encrypt callback: %v", err)
	}
	return signedRequest(t, http.MethodPost, "/im/wecom/"+testRoute, encrypted, "body")
}

func signedRequest(t *testing.T, method, path, encrypted, encryptedParameter string) *http.Request {
	t.Helper()
	return signedRequestWithTimestampAndParameter(t, method, path, encrypted, encryptedParameter, testNow.Unix())
}

func signedRequestWithTimestamp(t *testing.T, method, path, encrypted string, timestamp int64) *http.Request {
	t.Helper()
	return signedRequestWithTimestampAndParameter(t, method, path, encrypted, "encrypt", timestamp)
}

func signedRequestWithTimestampAndParameter(t *testing.T, method, path, encrypted, encryptedParameter string, timestamp int64) *http.Request {
	t.Helper()
	nonce := "nonce-1"
	timestampValue := strconv.FormatInt(timestamp, 10)
	signature := signForTest(testToken, timestampValue, nonce, encrypted)
	query := url.Values{
		"msg_signature": []string{signature},
		"timestamp":     []string{timestampValue},
		"nonce":         []string{nonce},
	}
	query.Set(encryptedParameter, encrypted)
	request := httptest.NewRequest(method, path+"?"+query.Encode(), strings.NewReader(`{"encrypt":"placeholder"}`))
	if method == http.MethodPost {
		request.Header.Set("Content-Type", "application/json")
		payload := []byte(fmt.Sprintf(`{"encrypt":%q}`, encrypted))
		request.Body = io.NopCloser(bytes.NewReader(payload))
	}
	return request
}

func signedQueryForTest(value string) string {
	return "msg_signature=invalid&timestamp=" + strconv.FormatInt(testNow.Unix(), 10) + "&nonce=nonce-1&encrypt=" + url.QueryEscape(value)
}

func signForTest(token, timestamp, nonce, encrypted string) string {
	values := []string{token, timestamp, nonce, encrypted}
	slicesSort(values)
	digest := sha1.Sum([]byte(strings.Join(values, "")))
	return hex.EncodeToString(digest[:])
}

func slicesSort(values []string) {
	for index := 1; index < len(values); index++ {
		for current := index; current > 0 && values[current] < values[current-1]; current-- {
			values[current], values[current-1] = values[current-1], values[current]
		}
	}
}

func base64Raw(value string) string {
	return base64.RawStdEncoding.EncodeToString([]byte(value))
}

func testBindingSnapshot() channels.BindingSnapshot {
	return channels.BindingSnapshot{Binding: channels.Binding{
		TenantID:         "tenant-a",
		AppID:            "app-a",
		BindingID:        testBindingID,
		Channel:          channels.ChannelWeCom,
		ExternalAccount:  testExternalBot,
		WebhookURL:       "https://example.test/im",
		TokenRef:         tenant.SecretRef{Name: "token"},
		SigningSecretRef: tenant.SecretRef{Name: "encoding-aes-key"},
		Secret:           tenant.SecretRef{Name: "unrelated-provider-secret"},
		PublicRouteID:    testRoute,
		BindingRevision:  3,
		Status:           channels.BindingActive,
	}}
}

func eventPayload(t *testing.T) []byte {
	t.Helper()
	return []byte(`{"eventtype":"template_card_event","template_card_event":{"event_key":"ok"}}`)
}

func testReply(operation channels.ReplyOperation, kind channels.ReplyKind) channels.Reply {
	return channels.Reply{
		TenantID:       "tenant-a",
		AppID:          "app-a",
		RequestID:      "request-1",
		SourceEventID:  "event-1",
		Channel:        channels.ChannelWeCom,
		BindingID:      testBindingID,
		ReplyID:        "reply-1",
		LogicalReplyID: "logical-1",
		PartNo:         1,
		Revision:       1,
		Operation:      operation,
		Kind:           kind,
		Target: channels.ReplyTarget{
			Kind:             channels.TargetKindMessage,
			InternalEntityID: "request-1",
		},
		Text: "hello",
	}
}

func independentEncryptedMessage(t *testing.T, key, prefix, message []byte, receiveID string) string {
	t.Helper()
	plaintext := make([]byte, aes.BlockSize+4+len(message)+len(receiveID))
	copy(plaintext, prefix)
	binary.BigEndian.PutUint32(plaintext[aes.BlockSize:aes.BlockSize+4], uint32(len(message)))
	copy(plaintext[aes.BlockSize+4:], message)
	copy(plaintext[aes.BlockSize+4+len(message):], receiveID)
	padding := 32 - len(plaintext)%32
	plaintext = append(plaintext, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatalf("new vector cipher: %v", err)
	}
	ciphertext := make([]byte, len(plaintext))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(ciphertext, plaintext)
	return base64.StdEncoding.EncodeToString(ciphertext)
}
