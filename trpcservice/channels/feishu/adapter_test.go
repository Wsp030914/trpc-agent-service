package feishu

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	testTenantID          = "tenant-feishu"
	testAppID             = "app-feishu"
	testBindingID         = "binding-feishu"
	testExternalAppID     = "cli-feishu"
	testExternalTenantKey = "tenant-key-feishu"
	testRouteID           = "r_feishu_test"
	testVerifyToken       = "verify-token"
	testEncryptKey        = "encrypt-key"
	testAppSecret         = "app-secret"
	testCallbackTime      = "1710000000000"
	testCallbackNonce     = "nonce-feishu"
)

var testCallbackNow = time.UnixMilli(1710000000000).UTC()

func TestAdapterAcceptsDirectMessage(t *testing.T) {
	adapter, admitter, _ := newTestAdapter(t)
	body := messageEventJSON(t, "om-direct", "ou-user", "p2p", "oc-private", "", "text", `{"text":"hello"}`)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, signedRequest(body))

	if response.Code != http.StatusOK || response.Body.String() != `{"code":0}` {
		t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
	}
	requests := admitter.requests()
	if len(requests) != 1 || requests[0].ChannelInput == nil {
		t.Fatalf("admission requests=%#v", requests)
	}
	input := requests[0].ChannelInput
	if input.TenantID != testTenantID || input.AppID != testAppID ||
		input.BindingID != testBindingID || input.MessageType != channels.MessageTypeText ||
		input.Text != "hello" || input.Conversation.Kind != channels.ConversationDirect {
		t.Fatalf("channel input=%#v", input)
	}
	mapping, ok := input.MappingInput()
	if !ok || mapping.ExternalSenderID != "ou-user" || mapping.ProviderSenderTarget != "open_id:ou-user" {
		t.Fatalf("mapping=%#v ok=%v", mapping, ok)
	}
}

func TestAdapterMapsGroupAndTopicMessages(t *testing.T) {
	tests := []struct {
		name          string
		chatType      string
		chatID        string
		threadID      string
		wantKind      channels.ConversationKind
		wantTarget    string
		wantThreadRef string
	}{
		{
			name:       "group",
			chatType:   "group",
			chatID:     "oc-group",
			wantKind:   channels.ConversationGroup,
			wantTarget: "chat_id:oc-group",
		},
		{
			name:          "topic",
			chatType:      "group",
			chatID:        "oc-group",
			threadID:      "omt-topic",
			wantKind:      channels.ConversationTopic,
			wantTarget:    "chat_id:oc-group",
			wantThreadRef: "thread_id:omt-topic",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, admitter, _ := newTestAdapter(t)
			body := messageEventJSON(t, "om-"+test.name, "ou-user", test.chatType, test.chatID, test.threadID, "text", `{"text":"hello"}`)
			response := httptest.NewRecorder()
			adapter.ServeHTTP(response, signedRequest(body))
			if response.Code != http.StatusOK {
				t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
			}
			requests := admitter.requests()
			if len(requests) != 1 || requests[0].ChannelInput == nil {
				t.Fatalf("admission requests=%#v", requests)
			}
			input := requests[0].ChannelInput
			if input.Conversation.Kind != test.wantKind {
				t.Fatalf("conversation=%#v", input.Conversation)
			}
			mapping, ok := input.MappingInput()
			if !ok || mapping.ExternalChatID != test.chatID || mapping.ProviderConversationTarget != test.wantTarget || mapping.ProviderThreadTarget != test.wantThreadRef {
				t.Fatalf("mapping=%#v ok=%v", mapping, ok)
			}
		})
	}
}

func TestAdapterReturnsChallengeWithoutAdmission(t *testing.T) {
	adapter, admitter, _ := newTestAdapter(t)
	body := []byte(`{"type":"url_verification","token":"verify-token","challenge":"challenge-value"}`)
	request := httptest.NewRequest(http.MethodPost, "/im/feishu/"+testRouteID, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, request)

	if response.Code != http.StatusOK || response.Body.String() != `{"challenge":"challenge-value"}` {
		t.Fatalf("challenge response status=%d body=%q", response.Code, response.Body.String())
	}
	if len(admitter.requests()) != 0 {
		t.Fatal("challenge created an admission")
	}
}

func TestAdapterRejectsInvalidSignatureTokenAndAccount(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*http.Request)
		mutateBody func([]byte) []byte
	}{
		{
			name: "signature",
			mutate: func(request *http.Request) {
				request.Header.Set(larkevent.EventSignature, "invalid")
			},
		},
		{
			name: "token",
			mutateBody: func(body []byte) []byte {
				return bytes.Replace(body, []byte(`"token":"verify-token"`), []byte(`"token":"wrong-token"`), 1)
			},
		},
		{
			name: "account",
			mutateBody: func(body []byte) []byte {
				return bytes.Replace(body, []byte(`"app_id":"cli-feishu"`), []byte(`"app_id":"cli-other"`), 1)
			},
		},
		{
			name: "tenant_key",
			mutateBody: func(body []byte) []byte {
				return bytes.Replace(body, []byte(`"tenant_key":"tenant-key-feishu"`), []byte(`"tenant_key":"tenant-key-other"`), 1)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, admitter, _ := newTestAdapter(t)
			body := messageEventJSON(t, "om-invalid-"+test.name, "ou-user", "p2p", "oc-private", "", "text", `{"text":"hello"}`)
			if test.mutateBody != nil {
				body = test.mutateBody(body)
			}
			request := signedRequest(body)
			if test.mutate != nil {
				test.mutate(request)
			}
			response := httptest.NewRecorder()
			adapter.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
			}
			if len(admitter.requests()) != 0 {
				t.Fatal("invalid callback reached Gateway")
			}
		})
	}
}

func TestAdapterRejectsStaleOrMalformedTimestamp(t *testing.T) {
	tests := []struct {
		name      string
		timestamp string
		remove    bool
	}{
		{name: "stale", timestamp: "1709990000"},
		{name: "future", timestamp: "1710010000"},
		{name: "malformed", timestamp: "not-a-timestamp"},
		{name: "missing", remove: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			adapter, admitter, _ := newTestAdapter(t)
			body := messageEventJSON(t, "om-timestamp-"+test.name, "ou-user", "p2p", "oc-private", "", "text", `{"text":"hello"}`)
			request := signedRequestAt(body, test.timestamp)
			if test.remove {
				request.Header.Del(larkevent.EventRequestTimestamp)
			}
			response := httptest.NewRecorder()
			adapter.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
			}
			if len(admitter.requests()) != 0 {
				t.Fatal("stale callback reached Gateway")
			}
		})
	}
}

func TestAdapterRejectsUnknownAndInactiveRoutes(t *testing.T) {
	adapter, _, resolver := newTestAdapter(t)
	unknown := httptest.NewRequest(http.MethodPost, "/im/feishu/r_missing", strings.NewReader(`{}`))
	unknown.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, unknown)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown route status=%d", response.Code)
	}
	resolver.snapshot.Status = channels.BindingSuspended
	body := messageEventJSON(t, "om-inactive", "ou-user", "p2p", "oc-private", "", "text", `{"text":"hello"}`)
	response = httptest.NewRecorder()
	adapter.ServeHTTP(response, signedRequest(body))
	if response.Code != http.StatusForbidden {
		t.Fatalf("inactive route status=%d", response.Code)
	}
}

func TestAdapterRequiresMediaIngestor(t *testing.T) {
	adapter, admitter, _ := newTestAdapter(t)
	body := messageEventJSON(t, "om-image", "ou-user", "p2p", "oc-private", "", "image", `{"image_key":"img-key"}`)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, signedRequest(body))
	if response.Code != http.StatusServiceUnavailable || len(admitter.requests()) != 0 {
		t.Fatalf("missing ingestor status=%d admissions=%d", response.Code, len(admitter.requests()))
	}
}

func TestAdapterPassesMediaOnlyThroughAttachmentBoundary(t *testing.T) {
	ingestor := &testAttachmentIngestor{}
	adapter, admitter, _ := newTestAdapter(t, WithAttachmentIngestor(ingestor))
	body := messageEventJSON(t, "om-image", "ou-user", "p2p", "oc-private", "", "image", `{"image_key":"img-key"}`)
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, signedRequest(body))
	if response.Code != http.StatusOK {
		t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
	}
	if len(ingestor.media) != 1 || ingestor.media[0].Kind != channels.MessageTypeImage || ingestor.media[0].Reference != "img-key" {
		t.Fatalf("media=%#v", ingestor.media)
	}
	requests := admitter.requests()
	if len(requests) != 1 || requests[0].ChannelInput == nil || len(requests[0].ChannelInput.ArtifactRefs) != 1 {
		t.Fatalf("admission requests=%#v", requests)
	}
}

func TestAdapterAdmitsVerifiedRecall(t *testing.T) {
	recall := &testRecallAdmitter{}
	adapter, admitter, _ := newTestAdapter(t, WithRecallAdmitter(recall))
	body := recallEventJSON(t, "om-recalled", "recall-event-1")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, signedRequest(body))
	if response.Code != http.StatusOK || response.Body.String() != `{"code":0}` {
		t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
	}
	if len(admitter.requests()) != 0 {
		t.Fatal("recall reached message admission")
	}
	requests := recall.requests()
	if len(requests) != 1 {
		t.Fatalf("recall requests=%#v", requests)
	}
	request := requests[0]
	if request.TenantID != testTenantID || request.AppID != testAppID ||
		request.BindingID != testBindingID || request.Channel != channels.ChannelFeishu ||
		request.ExternalEventID != "recall-event-1" || request.ExternalMessageID != "om-recalled" ||
		len(request.PayloadHash) != 32 {
		t.Fatalf("recall request=%#v", request)
	}
}

func TestAdapterDoesNotAckRecallWithoutDurableAdmitter(t *testing.T) {
	adapter, _, _ := newTestAdapter(t)
	body := recallEventJSON(t, "om-recalled", "recall-event-1")
	response := httptest.NewRecorder()
	adapter.ServeHTTP(response, signedRequest(body))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestNormalizeMessageContentSupportsTextImageAndFile(t *testing.T) {
	tests := []struct {
		name      string
		kind      string
		content   string
		wantType  channels.MessageType
		wantText  string
		wantMedia int
	}{
		{name: "text", kind: "text", content: `{"text":"hello"}`, wantType: channels.MessageTypeText, wantText: "hello"},
		{name: "image", kind: "image", content: `{"image_key":"img-key"}`, wantType: channels.MessageTypeImage, wantMedia: 1},
		{name: "file", kind: "file", content: `{"file_key":"file-key"}`, wantType: channels.MessageTypeFile, wantMedia: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotType, gotText, gotMedia, err := normalizeMessageContent(test.kind, test.content)
			if err != nil || gotType != test.wantType || gotText != test.wantText || len(gotMedia) != test.wantMedia {
				t.Fatalf("type=%s text=%q media=%#v err=%v", gotType, gotText, gotMedia, err)
			}
		})
	}
}

func TestOutboundClientUsesOfficialFeishuOpenAPI(t *testing.T) {
	var (
		mu       sync.Mutex
		paths    []string
		requests [][]byte
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		paths = append(paths, r.URL.Path)
		requests = append(requests, body)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"tenant_access_token":"tenant-token","expire":7200}}`))
		case "/open-apis/im/v1/messages":
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"message_id":"om-created"}}`))
		case "/open-apis/im/v1/messages/om-inbound/reply":
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"message_id":"om-reply"}}`))
		case "/open-apis/im/v1/messages/om-created":
			_, _ = w.Write([]byte(`{"code":0,"msg":"ok","data":{"message_id":"om-created"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	client, err := NewOutboundClient(
		context.Background(),
		testSecrets(),
		testBindingSnapshot(),
		WithBaseURL(server.URL),
		WithHTTPClient(server.Client()),
	)
	if err != nil {
		t.Fatalf("new outbound client: %v", err)
	}
	reply := testReply(channels.ReplyOperationSend, channels.ReplyKindText)
	receipt, err := client.SendOnce(context.Background(), reply, "message_id:om-inbound", channels.OutboundContext{})
	if err != nil || receipt.ProviderMessageID != "om-reply" {
		t.Fatalf("receipt=%#v err=%v", receipt, err)
	}

	mu.Lock()
	gotPaths := append([]string(nil), paths...)
	gotRequests := append([][]byte(nil), requests...)
	mu.Unlock()
	if !slices.Contains(gotPaths, "/open-apis/auth/v3/tenant_access_token/internal") || !slices.Contains(gotPaths, "/open-apis/im/v1/messages/om-inbound/reply") {
		t.Fatalf("sdk paths=%v", gotPaths)
	}
	var foundReply map[string]any
	for index, path := range gotPaths {
		if path == "/open-apis/im/v1/messages/om-inbound/reply" {
			if err := json.Unmarshal(gotRequests[index], &foundReply); err != nil {
				t.Fatalf("decode reply request: %v", err)
			}
		}
	}
	if foundReply["msg_type"] != "text" || foundReply["uuid"] != reply.ReplyID {
		t.Fatalf("reply request=%#v", foundReply)
	}
}

func TestReceiptRequiresProviderMessageID(t *testing.T) {
	receipt, err := receiptFromID(nil)
	var providerErr *ProviderSendError
	if err == nil || receipt != (channels.ProviderReceipt{}) || !errors.As(err, &providerErr) || !errors.Is(err, errFeishuProviderMessageID) {
		t.Fatalf("receipt=%#v err=%v provider_err=%#v", receipt, err, providerErr)
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
		return "", errors.New("secret not found")
	}
	return value, nil
}

type testAdmitter struct {
	mu       sync.Mutex
	recorded []gateway.AdmissionRequest
}

func (a *testAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.mu.Lock()
	a.recorded = append(a.recorded, request)
	a.mu.Unlock()
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

type testRecallAdmitter struct {
	mu           sync.Mutex
	requestsList []channels.RecallRequest
}

func (a *testRecallAdmitter) AdmitRecall(_ context.Context, request channels.RecallRequest) (channels.RecallResult, error) {
	a.mu.Lock()
	a.requestsList = append(a.requestsList, request)
	a.mu.Unlock()
	return channels.RecallResult{RequestID: "request-1", ExecutionStatus: "CANCELED"}, nil
}

func (a *testRecallAdmitter) requests() []channels.RecallRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]channels.RecallRequest(nil), a.requestsList...)
}

func (i *testAttachmentIngestor) Prepare(_ context.Context, input channels.ChannelInput, media []channels.ProviderMediaRef) (channels.ChannelInput, error) {
	i.media = append([]channels.ProviderMediaRef(nil), media...)
	input.ArtifactRefs = []string{"artifact://feishu/test"}
	return input, nil
}

func newTestAdapter(t *testing.T, opts ...AdapterOption) (*Adapter, *testAdmitter, *testRouteResolver) {
	t.Helper()
	resolver := &testRouteResolver{snapshot: testBindingSnapshot()}
	admitter := &testAdmitter{}
	admissionGateway := gateway.New(gateway.WithAdmitter(admitter))
	options := append(opts, WithClock(func() time.Time { return testCallbackNow }))
	adapter, err := NewAdapter(resolver, admissionGateway, platformsecret.SecretProvider(testSecrets()), options...)
	if err != nil {
		t.Fatalf("new adapter: %v", err)
	}
	return adapter, admitter, resolver
}

func testBindingSnapshot() channels.BindingSnapshot {
	return channels.BindingSnapshot{Binding: channels.Binding{
		TenantID:             testTenantID,
		AppID:                testAppID,
		BindingID:            testBindingID,
		Channel:              channels.ChannelFeishu,
		ExternalAccount:      testExternalAppID,
		ExternalAccountScope: testExternalTenantKey,
		WebhookURL:           "https://example.test/im/feishu/" + testRouteID,
		TokenRef:             tenant.SecretRef{Name: "verification-token"},
		SigningSecretRef:     tenant.SecretRef{Name: "encrypt-key"},
		Secret:               tenant.SecretRef{Name: "app-secret"},
		PublicRouteID:        testRouteID,
		BindingRevision:      3,
		Status:               channels.BindingActive,
	}}
}

func testSecrets() testSecretProvider {
	return testSecretProvider{values: map[string]string{
		"verification-token": testVerifyToken,
		"encrypt-key":        testEncryptKey,
		"app-secret":         testAppSecret,
	}}
}

func messageEventJSON(
	t *testing.T,
	messageID, senderID, chatType, chatID, threadID, messageType, content string,
) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":    "event-" + messageID,
			"event_type":  feishuMessageEventType,
			"app_id":      testExternalAppID,
			"tenant_key":  testExternalTenantKey,
			"create_time": testCallbackTime,
			"token":       testVerifyToken,
		},
		"event": map[string]any{
			"sender": map[string]any{
				"sender_id":   map[string]any{"open_id": senderID},
				"sender_type": "user",
			},
			"message": map[string]any{
				"message_id":   messageID,
				"create_time":  testCallbackTime,
				"chat_id":      chatID,
				"thread_id":    threadID,
				"chat_type":    chatType,
				"message_type": messageType,
				"content":      content,
			},
		},
	})
	if err != nil {
		t.Fatalf("marshal message event: %v", err)
	}
	return payload
}

func recallEventJSON(t *testing.T, messageID, eventID string) []byte {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"schema": "2.0",
		"header": map[string]any{
			"event_id":    eventID,
			"event_type":  feishuRecallEventType,
			"app_id":      testExternalAppID,
			"tenant_key":  testExternalTenantKey,
			"create_time": "1710000000",
			"token":       testVerifyToken,
		},
		"event": map[string]any{
			"message_id":  messageID,
			"chat_id":     "oc-private",
			"recall_time": "1710000000000",
			"recall_type": "消息撤回",
		},
	})
	if err != nil {
		t.Fatalf("marshal recall event: %v", err)
	}
	return payload
}

func signedRequest(body []byte) *http.Request {
	return signedRequestAt(body, "1710000000")
}

func signedRequestAt(body []byte, timestamp string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/im/feishu/"+testRouteID, bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(larkevent.EventRequestTimestamp, timestamp)
	request.Header.Set(larkevent.EventRequestNonce, testCallbackNonce)
	request.Header.Set(larkevent.EventSignature, larkevent.Signature(timestamp, testCallbackNonce, testEncryptKey, string(body)))
	return request
}

func testReply(operation channels.ReplyOperation, kind channels.ReplyKind) channels.Reply {
	reply := channels.Reply{
		TenantID:        testTenantID,
		AppID:           testAppID,
		RequestID:       "request-1",
		SourceEventID:   "event-1",
		Channel:         channels.ChannelFeishu,
		BindingID:       testBindingID,
		BindingRevision: 3,
		ReplyID:         "reply-1",
		LogicalReplyID:  "logical-1",
		PartNo:          1,
		Revision:        1,
		Operation:       operation,
		Kind:            kind,
		Target: channels.ReplyTarget{
			Kind:             channels.TargetKindMessage,
			InternalEntityID: "request-1",
		},
		Text: "hello",
	}
	if kind == channels.ReplyKindArtifact {
		reply.Text = ""
		reply.ArtifactRef = "artifact://feishu/test"
	}
	return reply
}

var _ channels.AttachmentIngestor = (*testAttachmentIngestor)(nil)
var _ channels.ProviderOutboundClient = (*OutboundClient)(nil)
