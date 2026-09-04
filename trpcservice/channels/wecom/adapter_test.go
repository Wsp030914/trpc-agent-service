package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestHandleMessageUsesBotIDBindingAndSendsChannelInputToGateway(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newWeComRecordingAdmitter()
	adapter := newWeComTestAdapter(t, source, admitter, testWeComSecrets{
		key:   binding,
		value: "bot-secret-a",
	})

	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), Message{
		MessageID:   "msg-1",
		AIBotID:     "bot-a",
		ChatType:    "single",
		From:        MessageFrom{UserID: "user-a"},
		MessageType: "text",
		Text:        MessageText{Content: "hello"},
	}); err != nil {
		t.Fatalf("handle WeCom WS event: %v", err)
	}

	request := admitter.lastRequest(t)
	if request.Identity.Tenant.TenantID != "tenant-a" || request.Identity.Tenant.AppID != "support" || request.Identity.Tenant.BindingID != "binding-a" {
		t.Fatalf("admission scope = %#v, want tenant-a/support/binding-a", request.Identity.Tenant)
	}
	if request.ChannelInput == nil || request.ChannelInput.ExternalMessageID != "msg-1" || request.ChannelInput.Text != "hello" {
		t.Fatalf("channel input = %#v", request.ChannelInput)
	}
}

func TestHandleMessageDuplicateEventUsesGatewayIdempotency(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newWeComRecordingAdmitter()
	adapter := newWeComTestAdapter(t, source, admitter, testWeComSecrets{key: binding, value: "bot-secret"})
	event := Message{
		MessageID:   "msg-duplicate",
		AIBotID:     "bot-a",
		ChatType:    "single",
		From:        MessageFrom{UserID: "user-a"},
		MessageType: "text",
		Text:        MessageText{Content: "same"},
	}
	for range 2 {
		if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), event); err != nil {
			t.Fatalf("handle duplicate event: %v", err)
		}
	}
	if admitter.admittedCount() != 1 {
		t.Fatalf("admitted count = %d, want 1", admitter.admittedCount())
	}
}

func TestWebSocketClientAuthMessageReplyReconnectAndCancellation(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	reconnected := make(chan struct{})
	var reconnectOnce sync.Once
	serverErrors := make(chan error, 4)

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		connectionNumber := connections.Add(1)
		auth, err := readWeComFrame(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		if auth.Cmd != "aibot_subscribe" {
			serverErrors <- errors.New("first client frame was not aibot_subscribe")
			return
		}
		var credentials struct {
			BotID  string `json:"bot_id"`
			Secret string `json:"secret"`
		}
		if err := json.Unmarshal(auth.Body, &credentials); err != nil {
			serverErrors <- err
			return
		}
		if credentials.BotID != "bot-id" || credentials.Secret != "bot-secret" {
			serverErrors <- errors.New("wrong WeCom credentials")
			return
		}
		if err := writeWeComAck(conn, auth.Headers.ReqID); err != nil {
			serverErrors <- err
			return
		}
		if connectionNumber == 1 {
			message := Message{
				MessageID:   "msg-ws-1",
				AIBotID:     "bot-id",
				ChatType:    "single",
				From:        MessageFrom{UserID: "user-1"},
				MessageType: "text",
				Text:        MessageText{Content: "from websocket"},
			}
			if err := writeWeComFrame(conn, protocolFrame{
				Cmd:     "aibot_msg_callback",
				Headers: frameHeaders{ReqID: "incoming-1"},
				Body:    bodyBytes(message),
			}); err != nil {
				serverErrors <- err
				return
			}
			send, err := readWeComFrame(conn)
			if err != nil {
				serverErrors <- err
				return
			}
			if send.Cmd != "aibot_send_msg" {
				serverErrors <- errors.New("reply frame was not aibot_send_msg")
				return
			}
			var body struct {
				ChatID   string `json:"chatid"`
				MsgType  string `json:"msgtype"`
				Markdown struct {
					Content string `json:"content"`
				} `json:"markdown"`
			}
			if err := json.Unmarshal(send.Body, &body); err != nil {
				serverErrors <- err
				return
			}
			if body.ChatID != "user-1" || body.MsgType != "markdown" || body.Markdown.Content != "reply" {
				serverErrors <- errors.New("wrong aibot_send_msg body")
				return
			}
			if err := writeWeComAck(conn, send.Headers.ReqID); err != nil {
				serverErrors <- err
				return
			}
			return
		}
		reconnectOnce.Do(func() { close(reconnected) })
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	received := make(chan Message, 1)
	client, err := NewClient(
		"bot-id",
		"bot-secret",
		WithWebSocketURL("ws"+strings.TrimPrefix(server.URL, "http")),
		WithWebSocketReconnectDelay(time.Millisecond, 5*time.Millisecond),
		WithMessageHandler(func(_ context.Context, message Message) error {
			received <- message
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- client.Run(runCtx) }()

	select {
	case message := <-received:
		if message.MessageID != "msg-ws-1" || message.Text.Content != "from websocket" {
			t.Fatalf("received message = %#v", message)
		}
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom message")
	}
	if _, err := client.SendMessage(context.Background(), "user-1", "reply"); err != nil {
		t.Fatalf("send WeCom reply: %v", err)
	}
	select {
	case <-reconnected:
	case err := <-serverErrors:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom reconnect")
	}
	closeErr := client.Close(context.Background())
	if closeErr != nil {
		t.Fatalf("close WeCom client: %v", closeErr)
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for WeCom shutdown")
	}
	select {
	case err := <-serverErrors:
		t.Fatal(err)
	default:
	}
}

func TestOutboundClientSendsThroughBindingScopedSender(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	sender := &recordingWeComSender{messageID: "provider-msg-1"}
	client, err := NewOutboundClient(
		context.Background(),
		testWeComSecrets{key: binding, value: "bot-secret"},
		binding.Snapshot(),
		WithMessageSender(sender),
	)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := client.SendOnce(context.Background(), channels.Reply{
		TenantID:        binding.TenantID,
		AppID:           binding.AppID,
		RequestID:       "request-1",
		SourceEventID:   "msg-1",
		Channel:         channels.ChannelWeCom,
		BindingID:       binding.BindingID,
		BindingRevision: binding.BindingRevision,
		ReplyID:         "reply-1",
		Revision:        1,
		Target:          channels.ReplyTarget{Kind: channels.TargetKindUser, InternalEntityID: "entity-1"},
		Text:            "answer",
	}, "user-1")
	if err != nil {
		t.Fatalf("send reply: %v", err)
	}
	if receipt.ProviderMessageID != "provider-msg-1" || sender.target != "user-1" || sender.text != "answer" {
		t.Fatalf("receipt=%#v sender=%#v", receipt, sender)
	}
}

type recordingWeComSender struct {
	target    string
	text      string
	messageID string
}

func (s *recordingWeComSender) SendMessage(_ context.Context, target, text string) (string, error) {
	s.target = target
	s.text = text
	return s.messageID, nil
}

type wecomRecordingAdmitter struct {
	mu       sync.Mutex
	requests []gateway.AdmissionRequest
	results  map[string]gateway.AdmissionResult
}

func newWeComRecordingAdmitter() *wecomRecordingAdmitter {
	return &wecomRecordingAdmitter{results: make(map[string]gateway.AdmissionResult)}
}

func (a *wecomRecordingAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, request)
	if result, ok := a.results[request.IdempotencyKey]; ok {
		result.Replayed = true
		return result, nil
	}
	result := gateway.AdmissionResult{
		RequestID:     request.RequestID,
		ConfigVersion: "v1",
		TurnSeq:       int64(len(a.results) + 1),
		Status:        gateway.AdmissionStatusAdmitted,
	}
	a.results[request.IdempotencyKey] = result
	return result, nil
}

func (a *wecomRecordingAdmitter) lastRequest(t *testing.T) gateway.AdmissionRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.requests) == 0 {
		t.Fatal("gateway received no request")
	}
	return a.requests[len(a.requests)-1]
}

func (a *wecomRecordingAdmitter) admittedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.results)
}

type testWeComSecrets struct {
	key   channels.Binding
	value string
}

func (p testWeComSecrets) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	if scope.TenantID == p.key.TenantID && scope.AppID == p.key.AppID && ref.Name == p.key.Secret.Name {
		return p.value, nil
	}
	return "", errors.New("test WeCom secret not found")
}

func newWeComTestAdapter(
	t *testing.T,
	source channels.BindingSource,
	admitter *wecomRecordingAdmitter,
	secrets platformsecret.SecretProvider,
) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(source, gateway.New(admitter), secrets)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func testWeComBinding(tenantID, appID, bindingID, botID, secret string) channels.Binding {
	return channels.Binding{
		TenantID:        tenantID,
		AppID:           appID,
		BindingID:       bindingID,
		Channel:         channels.ChannelWeCom,
		ExternalAccount: botID,
		Secret:          tenant.SecretRef{Name: secret, Version: "v1"},
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}

func readWeComFrame(conn *websocket.Conn) (protocolFrame, error) {
	_, payload, err := conn.ReadMessage()
	if err != nil {
		return protocolFrame{}, err
	}
	var frame protocolFrame
	if err := json.Unmarshal(payload, &frame); err != nil {
		return protocolFrame{}, err
	}
	return frame, nil
}

func writeWeComFrame(conn *websocket.Conn, frame protocolFrame) error {
	payload, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, payload)
}

func writeWeComAck(conn *websocket.Conn, requestID string) error {
	return writeWeComFrame(conn, protocolFrame{
		Headers: frameHeaders{ReqID: requestID},
		ErrCode: 0,
		ErrMsg:  "ok",
	})
}

var _ platformsecret.SecretProvider = testWeComSecrets{}
