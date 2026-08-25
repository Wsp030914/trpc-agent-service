package worker_test

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestWorkerPrepareResolvesBackendWithoutStickySession(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())

	exec, err := w.Prepare(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Config.BackendConfig.Name != "shared" {
		t.Fatalf("backend_config name = %q, want shared", exec.Config.BackendConfig.Name)
	}
	if exec.Config.Version != "v1" {
		t.Fatalf("config version = %q, want v1", exec.Config.Version)
	}
	if exec.TenantSource != gateway.TenantSourceAuthenticatedClaims {
		t.Fatalf("tenant source = %q, want authenticated claims", exec.TenantSource)
	}
	const want = "tenant:tenant-a:app:support:session:principal-1:session-1"
	if exec.PartitionKey != want {
		t.Fatalf("partition key = %q, want %q", exec.PartitionKey, want)
	}
	if exec.Storage.Session.Ref.Kind != tenant.BackendSQL {
		t.Fatalf("session backend kind = %q, want sql", exec.Storage.Session.Ref.Kind)
	}
	if exec.Storage.Memory.Ref.Name != "memory-redis" {
		t.Fatalf("memory backend name = %q, want memory-redis", exec.Storage.Memory.Ref.Name)
	}
	storageKey, err := exec.Storage.Session.Key(
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
	)
	if err != nil {
		t.Fatalf("storage key: %v", err)
	}
	if storageKey != want {
		t.Fatalf("storage key = %q, want %q", storageKey, want)
	}
}

func TestWorkersPrepareSameJobWithoutNodeAffinity(t *testing.T) {
	backend := sharedBackendConfig()
	workers := []worker.Worker{
		testWorker(t, backend),
		testWorker(t, backend),
	}
	job := testJob("request-1", "tenant-a", "session-1")

	first, err := workers[0].Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("first worker prepare: %v", err)
	}
	second, err := workers[1].Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("second worker prepare: %v", err)
	}
	if first.PartitionKey != second.PartitionKey {
		t.Fatalf("partition keys differ: %q and %q", first.PartitionKey, second.PartitionKey)
	}
	if !reflect.DeepEqual(first.Config, second.Config) {
		t.Fatalf("app configs differ: %#v and %#v", first.Config, second.Config)
	}
	if !reflect.DeepEqual(first.Storage, second.Storage) {
		t.Fatalf("storage handles differ: %#v and %#v", first.Storage, second.Storage)
	}
}

func TestWorkerResolvesExactConfigVersion(t *testing.T) {
	backend := sharedBackendConfig()
	v1 := testAppConfig("tenant-a", backend)
	v2 := testAppConfig("tenant-a", backend)
	v2.Version = "v2"
	v2.Model.Model = "gpt-4.1"
	configs, err := config.NewStaticResolver(v1, v2)
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	w := worker.Worker{
		Config:  configs,
		Storage: storage.StaticResolver{},
	}
	job := testJob("request-1", "tenant-a", "session-1")
	job.Tenant.ConfigVersion = "v2"

	exec, err := w.Prepare(context.Background(), job)
	if err != nil {
		t.Fatalf("prepare job: %v", err)
	}
	if exec.Config.Version != "v2" || exec.Config.Model.Model != "gpt-4.1" {
		t.Fatalf("resolved config = %q/%q, want v2/gpt-4.1", exec.Config.Version, exec.Config.Model.Model)
	}
}

func TestWorkerRequiresConfigResolver(t *testing.T) {
	w := worker.Worker{}
	if _, err := w.Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded without config resolver")
	}
}

func TestWorkerRequiresStorageResolver(t *testing.T) {
	configs, err := config.NewStaticResolver(testAppConfig("tenant-a", sharedBackendConfig()))
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	w := worker.Worker{Config: configs}

	if _, err := w.Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded without storage resolver")
	}
}

func TestWorkerRejectsInvalidResolvedConfig(t *testing.T) {
	w := worker.Worker{
		Config: invalidConfigResolver{
			config: tenant.AppConfig{
				TenantID: "tenant-a",
				AppID:    "support",
				Version:  "v1",
			},
		},
		Storage: storage.StaticResolver{},
	}

	if _, err := w.Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded with invalid resolved config")
	}
}

func TestWorkerRejectsMismatchedStorageHandles(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())
	w.Storage = mismatchedStorageResolver{}

	if _, err := w.Prepare(
		context.Background(),
		testJob("request-1", "tenant-a", "session-1"),
	); err == nil {
		t.Fatal("prepare succeeded with mismatched storage handles")
	}
}

func TestWorkerRunCallsRunnerAndDrainsEvents(t *testing.T) {
	runner := &recordingRunner{
		events: []*event.Event{
			event.New("invocation-1", "assistant"),
			runnerCompletionEvent(),
		},
	}
	sink := &recordingEventSink{}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: runner}
	w.Events = sink

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if runner.userID != "principal-1" {
		t.Fatalf("runner user id = %q, want principal-1", runner.userID)
	}
	if runner.sessionID != "session-1" {
		t.Fatalf("runner session id = %q, want session-1", runner.sessionID)
	}
	if runner.message.Role != model.RoleUser || runner.message.Content != "hello" {
		t.Fatalf("runner message = %#v, want user hello", runner.message)
	}
	if runner.options.RequestID != "request-1" {
		t.Fatalf("runner request id = %q, want request-1", runner.options.RequestID)
	}
	if runner.options.AppName != "tenant:tenant-a:app:support:runner" {
		t.Fatalf(
			"runner app name = %q, want tenant:tenant-a:app:support:runner",
			runner.options.AppName,
		)
	}
	if got := runner.options.RuntimeState["tenant_id"]; got != "tenant-a" {
		t.Fatalf("runtime tenant_id = %v, want tenant-a", got)
	}
	if got := runner.options.RuntimeState["user_id"]; got != "user-1" {
		t.Fatalf("runtime user_id = %v, want user-1", got)
	}
	if got := runner.options.RuntimeState["session_principal_id"]; got != "principal-1" {
		t.Fatalf("runtime session_principal_id = %v, want principal-1", got)
	}
	if result.EventCount != 2 {
		t.Fatalf("event count = %d, want 2", result.EventCount)
	}
	if !result.RunnerCompleted {
		t.Fatal("runner completion was not observed")
	}
	if len(sink.events) != 2 {
		t.Fatalf("sink event count = %d, want 2", len(sink.events))
	}
}

func TestWorkerRunKeepsPrivateSessionsSeparate(t *testing.T) {
	w, sessions := sessionContractWorker(t)
	const sessionID = "private-session"

	first := testJob("request-1", "tenant-a", sessionID)
	first.Tenant.UserID = "user-1"
	first.Tenant.SessionPrincipalID = "user-1"
	first.Message.Text = "from user 1"
	if _, err := w.Run(context.Background(), first); err != nil {
		t.Fatalf("run first private message: %v", err)
	}

	firstKey := session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    "user-1",
		SessionID: sessionID,
	}
	firstSession, err := sessions.GetSession(context.Background(), firstKey)
	if err != nil {
		t.Fatalf("get first private session: %v", err)
	}
	if firstSession == nil {
		t.Fatal("first private session was not created")
	}
	firstEventCount := len(firstSession.GetEvents())

	second := testJob("request-2", "tenant-a", sessionID)
	second.Tenant.UserID = "user-2"
	second.Tenant.SessionPrincipalID = "user-2"
	second.Message.Text = "from user 2"
	if _, err := w.Run(context.Background(), second); err != nil {
		t.Fatalf("run second private message: %v", err)
	}

	firstSession, err = sessions.GetSession(context.Background(), firstKey)
	if err != nil {
		t.Fatalf("get first private session again: %v", err)
	}
	if got := len(firstSession.GetEvents()); got != firstEventCount {
		t.Fatalf("first private session event count = %d, want %d", got, firstEventCount)
	}
	secondSession, err := sessions.GetSession(context.Background(), session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    "user-2",
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("get second private session: %v", err)
	}
	if secondSession == nil {
		t.Fatal("second private session was not created")
	}
}

func TestWorkerRunSharesGroupSessionAcrossUsers(t *testing.T) {
	w, sessions := sessionContractWorker(t)
	const (
		principalID = "group-1"
		sessionID   = "group-session"
	)

	first := testJob("request-1", "tenant-a", sessionID)
	first.Tenant.UserID = "user-1"
	first.Tenant.SessionPrincipalID = principalID
	first.Message.Text = "from user 1"
	if _, err := w.Run(context.Background(), first); err != nil {
		t.Fatalf("run first group message: %v", err)
	}

	groupKey := session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    principalID,
		SessionID: sessionID,
	}
	groupSession, err := sessions.GetSession(context.Background(), groupKey)
	if err != nil {
		t.Fatalf("get group session: %v", err)
	}
	if groupSession == nil {
		t.Fatal("group session was not created")
	}
	firstEventCount := len(groupSession.GetEvents())

	second := testJob("request-2", "tenant-a", sessionID)
	second.Tenant.UserID = "user-2"
	second.Tenant.SessionPrincipalID = principalID
	second.Message.Text = "from user 2"
	if _, err := w.Run(context.Background(), second); err != nil {
		t.Fatalf("run second group message: %v", err)
	}

	groupSession, err = sessions.GetSession(context.Background(), groupKey)
	if err != nil {
		t.Fatalf("get shared group session: %v", err)
	}
	if got := len(groupSession.GetEvents()); got <= firstEventCount {
		t.Fatalf("group session event count = %d, want more than %d", got, firstEventCount)
	}
	userSession, err := sessions.GetSession(context.Background(), session.Key{
		AppName:   "tenant:tenant-a:app:support:runner",
		UserID:    "user-2",
		SessionID: sessionID,
	})
	if err != nil {
		t.Fatalf("get real-user session: %v", err)
	}
	if userSession != nil {
		t.Fatal("group message created a session scoped to the real user")
	}
}

func TestNewWorkerAppliesRuntimeOptions(t *testing.T) {
	configs, err := config.NewStaticResolver(testAppConfig("tenant-a", sharedBackendConfig()))
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	runner := &recordingRunner{
		events: []*event.Event{runnerCompletionEvent()},
	}
	sink := &recordingEventSink{}
	w := worker.New(
		configs,
		storage.StaticResolver{},
		worker.WithRunner(staticRunnerResolver{runner: runner}),
		worker.WithEventSink(sink),
		worker.WithEventSinkTimeout(time.Millisecond),
	)

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if !result.RunnerCompleted {
		t.Fatal("runner completion was not observed")
	}
	if len(sink.events) != 1 {
		t.Fatalf("sink event count = %d, want 1", len(sink.events))
	}
	if w.EventSinkTimeout != time.Millisecond {
		t.Fatalf("event sink timeout = %s, want %s", w.EventSinkTimeout, time.Millisecond)
	}
}

func TestWorkerRunDrainsEventsAfterSinkError(t *testing.T) {
	wantErr := errors.New("sink failed")
	runner := &recordingRunner{
		events: []*event.Event{
			event.New("invocation-1", "assistant"),
			runnerCompletionEvent(),
		},
	}
	sink := &recordingEventSink{err: wantErr}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: runner}
	w.Events = sink

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("run error = %v, want sink error", err)
	}
	if result.EventCount != 2 {
		t.Fatalf("event count = %d, want 2", result.EventCount)
	}
	if len(sink.events) != 2 {
		t.Fatalf("sink event count = %d, want 2", len(sink.events))
	}
}

func TestWorkerRunReturnsRunnerCompletionError(t *testing.T) {
	wantErr := &model.ResponseError{
		Type:    model.ErrorTypeRunError,
		Message: "runner failed",
	}
	runner := &recordingRunner{
		events: []*event.Event{{
			Response: &model.Response{
				Object: model.ObjectTypeRunnerCompletion,
				Done:   true,
				Error:  wantErr,
			},
		}},
	}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: runner}

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if !errors.Is(err, wantErr) {
		t.Fatalf("run error = %v, want runner error", err)
	}
	if !result.RunnerCompleted {
		t.Fatal("runner completion was not observed")
	}
	if result.EventCount != 1 {
		t.Fatalf("event count = %d, want 1", result.EventCount)
	}
}

func TestWorkerRunSkipsNilRunnerEvents(t *testing.T) {
	runner := &recordingRunner{
		events: []*event.Event{
			nil,
			runnerCompletionEvent(),
		},
	}
	sink := &recordingEventSink{}
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: runner}
	w.Events = sink

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if err != nil {
		t.Fatalf("run job: %v", err)
	}
	if result.EventCount != 1 {
		t.Fatalf("event count = %d, want 1", result.EventCount)
	}
	if len(sink.events) != 1 {
		t.Fatalf("sink event count = %d, want 1", len(sink.events))
	}
}

func TestWorkerRunDrainsEventsAfterSinkTimeout(t *testing.T) {
	runner := &recordingRunner{
		events: []*event.Event{
			event.New("invocation-1", "assistant"),
			runnerCompletionEvent(),
		},
	}
	sink := &blockingEventSink{}

	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: runner}
	w.Events = sink
	w.EventSinkTimeout = time.Millisecond

	result, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("run error = %v, want deadline exceeded", err)
	}
	if result.EventCount != 2 {
		t.Fatalf("event count = %d, want 2", result.EventCount)
	}
	if got := sink.calls.Load(); got > 1 {
		t.Fatalf("sink calls = %d, want at most 1 after timeout", got)
	}
}

func TestWorkerRunRequiresRunnerResolver(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())

	if _, err := w.Run(context.Background(), testJob("request-1", "tenant-a", "session-1")); err == nil {
		t.Fatal("run succeeded without runner resolver")
	}
}

func TestWorkerRunRequiresUserID(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: &recordingRunner{}}
	job := testJob("request-1", "tenant-a", "session-1")
	job.Tenant.UserID = ""

	if _, err := w.Run(context.Background(), job); err == nil {
		t.Fatal("run succeeded without user_id")
	}
}

func TestWorkerRunRejectsUnsupportedArtifactRefs(t *testing.T) {
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: &recordingRunner{}}
	job := testJob("request-1", "tenant-a", "session-1")
	job.Message.ArtifactRefs = []string{"artifact://file@1"}

	if _, err := w.Run(context.Background(), job); err == nil {
		t.Fatal("run succeeded with unsupported artifact refs")
	}
}

func testJob(requestID, tenantID, sessionID string) gateway.Job {
	return gateway.Job{
		RequestID:    requestID,
		TenantSource: gateway.TenantSourceAuthenticatedClaims,
		Tenant: tenant.RuntimeContext{
			TenantID:           tenantID,
			AppID:              "support",
			ConfigVersion:      "v1",
			SessionID:          sessionID,
			SessionPrincipalID: "principal-1",
			UserID:             "user-1",
			TraceID:            "trace-1",
		},
		Message: gateway.Message{Text: "hello"},
	}
}

type invalidConfigResolver struct {
	config tenant.AppConfig
}

func (r invalidConfigResolver) ResolveAppConfig(
	_ context.Context,
	_,
	_,
	_ string,
) (tenant.AppConfig, error) {
	return r.config, nil
}

type mismatchedStorageResolver struct{}

func (mismatchedStorageResolver) Resolve(
	_ context.Context,
	_ tenant.RuntimeContext,
	backend tenant.BackendConfig,
) (storage.Handles, error) {
	handles, err := (storage.StaticResolver{}).Resolve(
		context.Background(),
		testJob("request-1", "tenant-b", "session-1").Tenant,
		backend,
	)
	if err != nil {
		return storage.Handles{}, err
	}
	return handles, nil
}

func sharedBackendConfig() tenant.BackendConfig {
	return tenant.BackendConfig{
		Name: "shared",
		Session: tenant.BackendRef{
			Kind:    tenant.BackendSQL,
			Name:    "session-sql",
			Options: map[string]string{"schema": "agent"},
		},
		Memory: tenant.BackendRef{
			Kind: tenant.BackendRedis,
			Name: "memory-redis",
		},
	}
}

func testWorker(t *testing.T, backend tenant.BackendConfig) worker.Worker {
	t.Helper()
	configs, err := config.NewStaticResolver(testAppConfig("tenant-a", backend))
	if err != nil {
		t.Fatalf("new config resolver: %v", err)
	}
	return *worker.New(configs, storage.StaticResolver{})
}

func sessionContractWorker(t *testing.T) (worker.Worker, *sessioninmemory.SessionService) {
	t.Helper()
	sessions := sessioninmemory.NewSessionService()
	r := runner.NewRunner(
		"worker-session-contract",
		&sessionContractAgent{name: "assistant"},
		runner.WithSessionService(sessions),
	)
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close runner: %v", err)
		}
	})
	w := testWorker(t, sharedBackendConfig())
	w.Runner = staticRunnerResolver{runner: r}
	return w, sessions
}

func testAppConfig(tenantID string, backend tenant.BackendConfig) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: tenantID,
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider: "openai",
			Model:    "gpt-4.1-mini",
		},
		BackendConfig: backend,
	}
}

type staticRunnerResolver struct {
	runner runner.Runner
}

func (r staticRunnerResolver) ResolveRunner(
	_ context.Context,
	_ worker.Execution,
) (runner.Runner, error) {
	return r.runner, nil
}

type recordingRunner struct {
	userID    string
	sessionID string
	message   model.Message
	options   agent.RunOptions
	events    []*event.Event
	err       error
}

type sessionContractAgent struct {
	name string
}

func (a *sessionContractAgent) Run(
	_ context.Context,
	invocation *agent.Invocation,
) (<-chan *event.Event, error) {
	ch := make(chan *event.Event, 1)
	ch <- event.NewResponseEvent(
		invocation.InvocationID,
		a.name,
		&model.Response{
			Done: true,
			Choices: []model.Choice{{
				Index:   0,
				Message: model.NewAssistantMessage("ok"),
			}},
		},
	)
	close(ch)
	return ch, nil
}

func (a *sessionContractAgent) Tools() []frameworktool.Tool {
	return nil
}

func (a *sessionContractAgent) Info() agent.Info {
	return agent.Info{Name: a.name}
}

func (*sessionContractAgent) SubAgents() []agent.Agent {
	return nil
}

func (*sessionContractAgent) FindSubAgent(string) agent.Agent {
	return nil
}

func (r *recordingRunner) Run(
	_ context.Context,
	userID string,
	sessionID string,
	message model.Message,
	runOpts ...agent.RunOption,
) (<-chan *event.Event, error) {
	r.userID = userID
	r.sessionID = sessionID
	r.message = message
	r.options = agent.NewRunOptions(runOpts...)
	if r.err != nil {
		return nil, r.err
	}
	ch := make(chan *event.Event, len(r.events))
	for _, evt := range r.events {
		ch <- evt
	}
	close(ch)
	return ch, nil
}

func (*recordingRunner) Close() error {
	return nil
}

type recordingEventSink struct {
	events []*event.Event
	err    error
}

func (s *recordingEventSink) HandleRunnerEvent(
	_ context.Context,
	_ worker.Execution,
	evt *event.Event,
) error {
	s.events = append(s.events, evt)
	return s.err
}

type blockingEventSink struct {
	calls atomic.Int32
}

func (s *blockingEventSink) HandleRunnerEvent(
	ctx context.Context,
	_ worker.Execution,
	_ *event.Event,
) error {
	s.calls.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func runnerCompletionEvent() *event.Event {
	return &event.Event{
		Response: &model.Response{
			Object: model.ObjectTypeRunnerCompletion,
			Done:   true,
		},
	}
}
