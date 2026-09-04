package feishu

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkdispatcher "github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestHandleMessageUsesAppIDBindingAndSendsChannelInputToGateway(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support-a", "binding-a", "app-a", "app-secret-a")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newFeishuRecordingAdmitter()
	adapter := newFeishuTestAdapter(t, source, admitter, testFeishuSecrets{
		key:   binding,
		value: "app-secret-a",
	})

	if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), feishuTextEvent(
		"app-a", "om-1", "ou-1", "p2p", "hello",
	)); err != nil {
		t.Fatalf("handle Feishu WS event: %v", err)
	}

	request := admitter.lastRequest(t)
	if request.Identity.Tenant.TenantID != "tenant-a" || request.Identity.Tenant.AppID != "support-a" || request.Identity.Tenant.BindingID != "binding-a" {
		t.Fatalf("admission scope = %#v, want tenant-a/support-a/binding-a", request.Identity.Tenant)
	}
	if request.ChannelInput == nil || request.ChannelInput.ExternalMessageID != "om-1" || request.ChannelInput.Text != "hello" {
		t.Fatalf("channel input = %#v", request.ChannelInput)
	}
}

func TestHandleMessageDuplicateEventUsesGatewayIdempotency(t *testing.T) {
	binding := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "app-secret")
	source, err := config.NewStaticBindingResolver(binding)
	if err != nil {
		t.Fatal(err)
	}
	admitter := newFeishuRecordingAdmitter()
	adapter := newFeishuTestAdapter(t, source, admitter, testFeishuSecrets{
		key:   binding,
		value: "app-secret",
	})
	event := feishuTextEvent("app-a", "om-duplicate", "ou-1", "p2p", "same")
	for range 2 {
		if err := adapter.HandleMessage(context.Background(), binding.Snapshot(), event); err != nil {
			t.Fatalf("handle duplicate event: %v", err)
		}
	}
	if admitter.admittedCount() != 1 {
		t.Fatalf("admitted count = %d, want 1", admitter.admittedCount())
	}
}

func TestRunCreatesOneClientPerActiveBindingAndStopsOnCancellation(t *testing.T) {
	bindingA := testFeishuBinding("tenant-a", "support", "binding-a", "app-a", "secret-a")
	bindingB := testFeishuBinding("tenant-b", "support", "binding-b", "app-b", "secret-b")
	source, err := config.NewStaticBindingResolver(bindingA, bindingB)
	if err != nil {
		t.Fatal(err)
	}
	secrets := testFeishuSecrets{
		key:   bindingA,
		value: "secret-a",
		more:  map[string]string{testFeishuSecretKey(bindingB): "secret-b"},
	}
	started := make(chan feishuFakeClientInfo, 2)
	var clientsMu sync.Mutex
	var clients []*feishuBlockingClient
	adapter, err := NewAdapter(
		source,
		gateway.New(newFeishuRecordingAdmitter()),
		secrets,
		WithClock(func() time.Time { return time.Unix(100, 0) }),
		WithClientFactory(func(binding channels.BindingSnapshot, appSecret string, _ *larkdispatcher.EventDispatcher) Client {
			client := &feishuBlockingClient{started: make(chan struct{}), closed: make(chan struct{})}
			started <- feishuFakeClientInfo{binding: binding, secret: appSecret}
			clientsMu.Lock()
			clients = append(clients, client)
			clientsMu.Unlock()
			return client
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- adapter.Run(runCtx) }()
	seen := make(map[string]string)
	for range 2 {
		select {
		case info := <-started:
			seen[info.binding.TenantID+"/"+info.binding.BindingID] = info.secret
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for binding clients")
		}
	}
	if seen["tenant-a/binding-a"] != "secret-a" || seen["tenant-b/binding-b"] != "secret-b" {
		t.Fatalf("binding client secrets = %#v", seen)
	}
	cancel()
	select {
	case err := <-runDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("run error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for adapter shutdown")
	}
	clientsMu.Lock()
	clientCount := len(clients)
	clientsMu.Unlock()
	if clientCount != 2 {
		t.Fatalf("client count = %d, want 2", clientCount)
	}
}

type feishuFakeClientInfo struct {
	binding channels.BindingSnapshot
	secret  string
}

type feishuBlockingClient struct {
	started chan struct{}
	closed  chan struct{}
}

func (c *feishuBlockingClient) Start(ctx context.Context) error {
	close(c.started)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return nil
	}
}

func (c *feishuBlockingClient) CloseAndWait(context.Context) error {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

var _ Client = (*feishuBlockingClient)(nil)

type feishuRecordingAdmitter struct {
	mu       sync.Mutex
	requests []gateway.AdmissionRequest
	results  map[string]gateway.AdmissionResult
}

func newFeishuRecordingAdmitter() *feishuRecordingAdmitter {
	return &feishuRecordingAdmitter{results: make(map[string]gateway.AdmissionResult)}
}

func (a *feishuRecordingAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
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

func (a *feishuRecordingAdmitter) lastRequest(t *testing.T) gateway.AdmissionRequest {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.requests) == 0 {
		t.Fatal("gateway received no request")
	}
	return a.requests[len(a.requests)-1]
}

func (a *feishuRecordingAdmitter) admittedCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.results)
}

type testFeishuSecrets struct {
	key   channels.Binding
	value string
	more  map[string]string
}

func (p testFeishuSecrets) ResolveSecret(_ context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error) {
	key := scope.TenantID + "\x00" + scope.AppID + "\x00" + ref.Name
	if key == testFeishuSecretKey(p.key) {
		return p.value, nil
	}
	if value := p.more[key]; value != "" {
		return value, nil
	}
	return "", errors.New("test Feishu secret not found")
}

func testFeishuSecretKey(binding channels.Binding) string {
	return binding.TenantID + "\x00" + binding.AppID + "\x00" + binding.Secret.Name
}

func newFeishuTestAdapter(
	t *testing.T,
	source channels.BindingSource,
	admitter *feishuRecordingAdmitter,
	secrets platformsecret.SecretProvider,
) *Adapter {
	t.Helper()
	adapter, err := NewAdapter(source, gateway.New(admitter), secrets)
	if err != nil {
		t.Fatal(err)
	}
	return adapter
}

func testFeishuBinding(tenantID, appID, bindingID, externalAccount, secret string) channels.Binding {
	return channels.Binding{
		TenantID:        tenantID,
		AppID:           appID,
		BindingID:       bindingID,
		Channel:         channels.ChannelFeishu,
		ExternalAccount: externalAccount,
		Secret:          tenant.SecretRef{Name: secret, Version: "v1"},
		BindingRevision: 1,
		Status:          channels.BindingActive,
	}
}

func feishuTextEvent(appID, messageID, senderID, chatType, text string) *larkim.P2MessageReceiveV1 {
	return &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{
			EventID:   "event-" + messageID,
			EventType: feishuMessageEventType,
			AppID:     appID,
		}},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{SenderId: &larkim.UserId{OpenId: &senderID}},
			Message: &larkim.EventMessage{
				MessageId:   &messageID,
				ChatType:    &chatType,
				MessageType: stringPointer("text"),
				Content:     stringPointer(`{"text":"` + text + `"}`),
			},
		},
	}
}

func stringPointer(value string) *string {
	return &value
}

var _ platformsecret.SecretProvider = testFeishuSecrets{}
