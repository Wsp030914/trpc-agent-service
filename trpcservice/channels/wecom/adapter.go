package wecom

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/attribute"
)

const (
	defaultReconnectInitial  = time.Second
	defaultReconnectMax      = 30 * time.Second
	defaultReconcileInterval = time.Second
)

var errAttachmentIngestorRequired = errors.New("attachment ingestor is required")

// ClientRunner is the lifecycle surface used by the adapter. The production
// value is the official WeCom AI Bot protocol client implemented in websocket.go.
type ClientRunner interface {
	Run(context.Context) error
	Close(context.Context) error
}

// ClientFactory is a test seam for one binding-scoped protocol client.
type ClientFactory func(
	binding channels.BindingSnapshot,
	botSecret string,
	handler MessageHandler,
) (ClientRunner, error)

// AdapterOption configures a WeCom long-connection adapter.
type AdapterOption func(*Adapter) error

// WithClock supplies the clock used for inbound timestamps.
func WithClock(clock func() time.Time) AdapterOption {
	return func(adapter *Adapter) error {
		if clock == nil {
			return errors.New("clock is required")
		}
		adapter.now = clock
		return nil
	}
}

// WithAttachmentIngestor registers the IM-07 media materialization boundary.
func WithAttachmentIngestor(ingestor channels.AttachmentIngestor) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.attachmentIngestor = ingestor
		return nil
	}
}

// WithMetrics attaches the low-cardinality IM event metrics recorder.
func WithMetrics(recorder *platformmetrics.Recorder) AdapterOption {
	return func(adapter *Adapter) error {
		adapter.metrics = recorder
		return nil
	}
}

// WithClientFactory replaces only the protocol client constructor. Production
// code should leave it unset so the official WeCom protocol client is used.
func WithClientFactory(factory ClientFactory) AdapterOption {
	return func(adapter *Adapter) error {
		if factory == nil {
			return errors.New("client factory is required")
		}
		adapter.clientFactory = factory
		return nil
	}
}

// WithReconnectDelay configures the bounded adapter-level recovery delay.
func WithReconnectDelay(initial, maximum time.Duration) AdapterOption {
	return func(adapter *Adapter) error {
		if initial <= 0 || maximum < initial {
			return errors.New("reconnect delay is invalid")
		}
		adapter.reconnectInitial = initial
		adapter.reconnectMax = maximum
		return nil
	}
}

// WithReconcileInterval controls how often the adapter refreshes active
// binding snapshots. A short interval is useful for tests and deployments
// that need prompt control-plane changes without restarting the service.
func WithReconcileInterval(interval time.Duration) AdapterOption {
	return func(adapter *Adapter) error {
		if interval <= 0 {
			return errors.New("reconcile interval is invalid")
		}
		adapter.reconcileInterval = interval
		return nil
	}
}

// Adapter owns one authenticated WeCom long connection per active Binding and
// submits normalized messages to Gateway. It does not call Runner or own Reply
// Outbox delivery.
type Adapter struct {
	bindings           channels.BindingSource
	admissionGateway   *gateway.Gateway
	secrets            platformsecret.SecretProvider
	attachmentIngestor channels.AttachmentIngestor
	now                func() time.Time
	metrics            *platformmetrics.Recorder
	clientFactory      ClientFactory
	reconnectInitial   time.Duration
	reconnectMax       time.Duration
	reconcileInterval  time.Duration

	runMu     sync.Mutex
	runCancel context.CancelFunc
	runDone   chan struct{}
	stopMu    sync.Mutex
	clientsMu sync.Mutex
	clients   map[string]ClientRunner
	runsMu    sync.Mutex
	runs      map[string]*wecomBindingRun
}

type wecomBindingRun struct {
	snapshot channels.Binding
	cancel   context.CancelFunc
	done     chan error
	clientMu sync.Mutex
	client   *wecomClientHandle
}

type wecomClientHandle struct {
	client    ClientRunner
	closeOnce sync.Once
	closeErr  error
}

func (h *wecomClientHandle) close(ctx context.Context) error {
	if h == nil || h.client == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		h.closeErr = h.client.Close(ctx)
	})
	return h.closeErr
}

type wecomBindingRunResult struct {
	key string
	run *wecomBindingRun
	err error
}

// NewAdapter creates a WeCom AI Bot long-connection adapter. Binding.ExternalAccount
// is Bot ID and Binding.Secret is the Bot Secret reference.
func NewAdapter(
	bindings channels.BindingSource,
	admissionGateway *gateway.Gateway,
	secrets platformsecret.SecretProvider,
	opts ...AdapterOption,
) (*Adapter, error) {
	if bindings == nil {
		return nil, errors.New("binding source is required")
	}
	if admissionGateway == nil {
		return nil, errors.New("gateway is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	adapter := &Adapter{
		bindings:          bindings,
		admissionGateway:  admissionGateway,
		secrets:           secrets,
		now:               time.Now,
		clientFactory:     defaultClientFactory,
		reconnectInitial:  defaultReconnectInitial,
		reconnectMax:      defaultReconnectMax,
		reconcileInterval: defaultReconcileInterval,
		clients:           make(map[string]ClientRunner),
		runs:              make(map[string]*wecomBindingRun),
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

func defaultClientFactory(
	binding channels.BindingSnapshot,
	botSecret string,
	handler MessageHandler,
) (ClientRunner, error) {
	return NewClient(
		binding.ExternalAccount,
		botSecret,
		WithMessageHandler(handler),
		WithOnError(func(err error) {
			log.Printf("wecom inbound event failed: %s", platformlog.SafeError(err))
		}),
	)
}

// Run reconciles active bindings and starts one client for each. Binding
// additions and updates do not wait for unrelated long connections to exit;
// changed/removed snapshots are canceled and closed independently.
func (a *Adapter) Run(ctx context.Context) error {
	if a == nil || a.bindings == nil || a.admissionGateway == nil || a.secrets == nil {
		return errors.New("wecom adapter is not initialized")
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	a.runMu.Lock()
	if a.runCancel != nil {
		a.runMu.Unlock()
		cancel()
		return errors.New("wecom adapter is already running")
	}
	a.runCancel = cancel
	a.runDone = runDone
	a.runMu.Unlock()
	defer func() {
		cancel()
		close(runDone)
		a.runMu.Lock()
		a.runCancel = nil
		a.runDone = nil
		a.runMu.Unlock()
	}()
	results := make(chan wecomBindingRunResult, 32)
	ticker := time.NewTicker(a.reconcileInterval)
	defer ticker.Stop()
	for {
		bindings, err := a.bindings.ListActiveChannelBindings(runCtx, channels.ChannelWeCom)
		if err != nil {
			a.stopAllBindings(runCtx)
			return fmt.Errorf("list active wecom bindings: %w", err)
		}
		if err := a.reconcileBindings(runCtx, bindings, results); err != nil {
			a.stopAllBindings(runCtx)
			return err
		}
		select {
		case <-runCtx.Done():
			a.stopAllBindings(context.Background())
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return runCtx.Err()
		case result := <-results:
			a.finishBindingRun(result)
			if result.err != nil && !errors.Is(result.err, context.Canceled) {
				a.stopAllBindings(runCtx)
				return result.err
			}
		case <-ticker.C:
		}
	}
}

func (a *Adapter) reconcileBindings(
	ctx context.Context,
	bindings []channels.Binding,
	results chan<- wecomBindingRunResult,
) error {
	desired := make(map[string]channels.Binding, len(bindings))
	for _, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("wecom binding: %w", err)
		}
		desired[bindingKey(binding.Snapshot())] = binding
	}
	a.runsMu.Lock()
	current := make(map[string]*wecomBindingRun, len(a.runs))
	for key, run := range a.runs {
		current[key] = run
	}
	a.runsMu.Unlock()
	for key, run := range current {
		binding, ok := desired[key]
		if !ok {
			a.stopBinding(ctx, key, run)
			continue
		}
		if wecomBindingRuntimeEqual(run.snapshot, binding) {
			continue
		}
		a.stopBinding(ctx, key, run)
	}
	for key, binding := range desired {
		a.runsMu.Lock()
		run := a.runs[key]
		a.runsMu.Unlock()
		if run != nil {
			continue
		}
		bindingCtx, cancel := context.WithCancel(ctx)
		run = &wecomBindingRun{snapshot: binding, cancel: cancel, done: make(chan error, 1)}
		a.runsMu.Lock()
		if existing := a.runs[key]; existing != nil {
			a.runsMu.Unlock()
			cancel()
			continue
		}
		a.runs[key] = run
		a.runsMu.Unlock()
		go func(key string, run *wecomBindingRun, binding channels.Binding, bindingCtx context.Context) {
			err := a.runBinding(bindingCtx, binding, run)
			run.done <- err
			select {
			case results <- wecomBindingRunResult{key: key, run: run, err: err}:
			case <-bindingCtx.Done():
			}
		}(key, run, binding, bindingCtx)
	}
	return nil
}

func wecomBindingRuntimeEqual(left, right channels.Binding) bool {
	return left.TenantID == right.TenantID && left.AppID == right.AppID &&
		left.BindingID == right.BindingID && left.Channel == right.Channel &&
		left.ExternalAccount == right.ExternalAccount && left.Secret == right.Secret &&
		left.BindingRevision == right.BindingRevision && left.Status == right.Status
}

func (a *Adapter) finishBindingRun(result wecomBindingRunResult) {
	a.runsMu.Lock()
	if current := a.runs[result.key]; current == result.run {
		delete(a.runs, result.key)
	}
	a.runsMu.Unlock()
}

func (a *Adapter) stopAllBindings(ctx context.Context) {
	a.runsMu.Lock()
	current := make(map[string]*wecomBindingRun, len(a.runs))
	for key, run := range a.runs {
		current[key] = run
	}
	a.runsMu.Unlock()
	for key, run := range current {
		a.stopBinding(ctx, key, run)
	}
}

func (a *Adapter) stopBinding(ctx context.Context, key string, run *wecomBindingRun) {
	if run == nil {
		return
	}
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	a.runsMu.Lock()
	if current := a.runs[key]; current != run {
		a.runsMu.Unlock()
		return
	}
	a.runsMu.Unlock()
	run.clientMu.Lock()
	client := run.client
	run.clientMu.Unlock()
	run.cancel()
	if client != nil {
		closeWeComClient(ctx, client)
	}
	select {
	case <-run.done:
	case <-time.After(5 * time.Second):
	}
	a.runsMu.Lock()
	if current := a.runs[key]; current == run {
		delete(a.runs, key)
	}
	a.runsMu.Unlock()
}

func (a *Adapter) runBinding(ctx context.Context, binding channels.Binding, run *wecomBindingRun) error {
	if err := binding.Validate(); err != nil {
		return fmt.Errorf("wecom binding: %w", err)
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		current, err := a.bindings.ResolveBinding(ctx, binding.TenantID, binding.AppID, binding.BindingID)
		if err != nil {
			return fmt.Errorf("resolve wecom binding: %w", err)
		}
		if current.Status != channels.BindingActive {
			return nil
		}
		if current.Channel != channels.ChannelWeCom || current.BindingRevision != binding.BindingRevision || current.ExternalAccount != binding.ExternalAccount {
			if current.Channel != channels.ChannelWeCom {
				return nil
			}
			if err := current.Validate(); err != nil {
				return fmt.Errorf("updated wecom binding: %w", err)
			}
			binding = current
			continue
		}
		botSecret, err := a.secrets.ResolveSecret(ctx, current.Scope(), current.Secret)
		if err != nil {
			return fmt.Errorf("resolve wecom bot secret: %w", err)
		}
		if botSecret == "" {
			return errors.New("wecom bot secret is required")
		}
		snapshot := current.Snapshot()
		client, err := a.clientFactory(snapshot, botSecret, func(eventCtx context.Context, message Message) error {
			return a.HandleMessage(eventCtx, snapshot, message)
		})
		if err != nil {
			return fmt.Errorf("create wecom client: %w", err)
		}
		if client == nil {
			return errors.New("wecom client factory returned nil")
		}
		key := bindingKey(snapshot)
		handle := &wecomClientHandle{client: client}
		run.clientMu.Lock()
		run.client = handle
		run.clientMu.Unlock()
		a.trackClient(key, client)
		a.recordConnection(ctx, snapshot, channels.ConnectionReady, nil)
		runErr := client.Run(ctx)
		a.untrackClient(key, client)
		closeWeComClient(ctx, handle)
		run.clientMu.Lock()
		if run.client == handle {
			run.client = nil
		}
		run.clientMu.Unlock()
		if ctx.Err() != nil {
			a.recordConnection(context.WithoutCancel(ctx), snapshot, channels.ConnectionNotReady, nil)
			return ctx.Err()
		}
		if runErr != nil {
			a.recordConnection(ctx, snapshot, channels.ConnectionDegraded, runErr)
		}
		if err := waitReconnect(ctx, a.reconnectInitial, a.reconnectMax); err != nil {
			return err
		}
	}
}

func closeWeComClient(ctx context.Context, handle *wecomClientHandle) {
	if handle == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = handle.close(closeCtx)
}

func (a *Adapter) recordConnection(
	ctx context.Context,
	binding channels.BindingSnapshot,
	status channels.ConnectionStatus,
	cause error,
) {
	reporter, ok := a.bindings.(channels.ConnectionStatusReporter)
	if !ok {
		return
	}
	if err := reporter.RecordChannelConnection(ctx, binding.TenantID, binding.AppID, binding.BindingID, status, cause); err != nil {
		log.Printf("record wecom connection status failed: %s", platformlog.SafeError(err))
	}
}

// HandleMessage is the authenticated protocol event boundary and exercises
// the same conversion used by the live WebSocket client.
func (a *Adapter) HandleMessage(
	ctx context.Context,
	binding channels.BindingSnapshot,
	message Message,
) error {
	if err := a.ensureCurrentBinding(ctx, binding); err != nil {
		return err
	}
	envelope, err := normalizeMessage(binding, message)
	if err != nil {
		return err
	}
	input, err := a.channelInput(ctx, envelope)
	if err != nil {
		return err
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
	identityResolver, err := gateway.NewChannelBindingInputIdentityResolverFromBinding(binding, runtimeContext)
	if err != nil {
		return fmt.Errorf("wecom admission identity: %w", err)
	}
	eventCtx, span := platformtelemetry.StartSpan(ctx, "channel.event",
		attribute.String("channel", string(channels.ChannelWeCom)),
		attribute.String("binding_id", binding.BindingID),
	)
	defer span.End()
	admissionRequest := gateway.Request{
		RequestID:      requestID,
		IdempotencyKey: envelope.ExternalMessageID,
		Tenant:         identityResolver,
		ChannelInput:   &input,
	}
	if len(envelope.Media) > 0 {
		pinnedIngestor, ok := a.attachmentIngestor.(channels.PinnedAttachmentIngestor)
		if !ok {
			return errAttachmentIngestorRequired
		}
		media := append([]channels.ProviderMediaRef(nil), envelope.Media...)
		_, err = a.admissionGateway.HandleChannel(eventCtx, admissionRequest,
			func(
				prepareCtx context.Context,
				prepareInput channels.ChannelInput,
				configVersion string,
			) (channels.ChannelInput, func(context.Context) error, error) {
				return pinnedIngestor.PreparePinned(prepareCtx, prepareInput, media, configVersion)
			})
	} else {
		_, err = a.admissionGateway.Handle(eventCtx, admissionRequest)
	}
	if err != nil {
		platformtelemetry.MarkError(span, "admission", err)
	}
	if a.metrics != nil {
		a.metrics.RecordIMCallback(eventCtx, platformmetrics.Labels{Channel: string(channels.ChannelWeCom)}, errorType(err))
	}
	return err
}

func (a *Adapter) ensureCurrentBinding(ctx context.Context, snapshot channels.BindingSnapshot) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	current, err := a.bindings.ResolveBinding(ctx, snapshot.TenantID, snapshot.AppID, snapshot.BindingID)
	if err != nil {
		return err
	}
	if current.Status != channels.BindingActive {
		return channels.ErrBindingInactive
	}
	if current.Channel != snapshot.Channel || current.BindingRevision != snapshot.BindingRevision || current.ExternalAccount != snapshot.ExternalAccount {
		return gateway.ErrChannelBindingSnapshotStale
	}
	return nil
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
		Conversation:      channels.ChannelConversation{Kind: envelope.ConversationKind},
		MessageType:       envelope.MessageType,
		Text:              envelope.Text,
		ProviderTimestamp: envelope.ProviderTimestamp,
		ReceivedAt:        a.now().UTC(),
	}
	input, err := channels.NewChannelInput(input, envelope.mapping)
	if err != nil {
		return channels.ChannelInput{}, err
	}
	return input, nil
}

// Close gracefully stops every live binding client.
func (a *Adapter) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if ctx == nil {
		return errors.New("context is required")
	}
	a.runMu.Lock()
	runCancel := a.runCancel
	runDone := a.runDone
	a.runMu.Unlock()
	if runCancel != nil {
		runCancel()
		a.stopAllBindings(context.WithoutCancel(ctx))
		select {
		case <-runDone:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	a.clientsMu.Lock()
	clients := make([]ClientRunner, 0, len(a.clients))
	for _, client := range a.clients {
		clients = append(clients, client)
	}
	a.clientsMu.Unlock()
	var result error
	for _, client := range clients {
		result = errors.Join(result, client.Close(ctx))
	}
	return result
}

func (a *Adapter) trackClient(key string, client ClientRunner) {
	a.clientsMu.Lock()
	a.clients[key] = client
	a.clientsMu.Unlock()
}

func (a *Adapter) untrackClient(key string, client ClientRunner) {
	a.clientsMu.Lock()
	if current, ok := a.clients[key]; ok && current == client {
		delete(a.clients, key)
	}
	a.clientsMu.Unlock()
}

func bindingKey(binding channels.BindingSnapshot) string {
	return binding.TenantID + "\x00" + binding.AppID + "\x00" + binding.BindingID
}

func waitReconnect(ctx context.Context, initial, maximum time.Duration) error {
	if initial <= 0 {
		initial = defaultReconnectInitial
	}
	if maximum < initial {
		maximum = initial
	}
	timer := time.NewTimer(initial)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func errorType(err error) string {
	if err == nil {
		return ""
	}
	return "admission"
}
