package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	frameworkevent "trpc.group/trpc-go/trpc-agent-go/event"
	frameworkmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/noop"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestVisibleToolsFiltersBeforeAgentConstruction(t *testing.T) {
	tools, err := visibleTools(tenant.ToolPolicy{VisibleTools: []string{"safe"}}, []frameworktool.Tool{
		testTool{name: "safe"}, testTool{name: "hidden"},
	})
	if err != nil {
		t.Fatalf("filter visible tools: %v", err)
	}
	if len(tools) != 1 || tools[0].Declaration().Name != "safe" {
		t.Fatalf("visible tools = %#v", tools)
	}
}

func TestVisibleToolsRejectsConfiguredUnavailableTool(t *testing.T) {
	_, err := visibleTools(tenant.ToolPolicy{VisibleTools: []string{"missing"}}, []frameworktool.Tool{
		testTool{name: "available"},
	})
	if err == nil {
		t.Fatal("visible tools succeeded with unavailable configured tool")
	}
}

func TestRuntimeRunnerResolverUsesToolResolver(t *testing.T) {
	tools := &testToolResolver{tools: []frameworktool.Tool{testTool{name: "safe"}}}
	resolver, err := NewRuntimeRunnerResolver(
		testModelResolver{},
		testSessionResolver{},
		WithRuntimeToolResolver(tools),
	)
	if err != nil {
		t.Fatalf("new runtime runner resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })
	exec := testMemoryExecution("user-a")
	exec.Config.Tools = tenant.ToolPolicy{VisibleTools: []string{"safe"}}
	if _, err := resolver.ResolveRunner(context.Background(), exec); err != nil {
		t.Fatalf("resolve runner: %v", err)
	}
	if tools.calls != 1 {
		t.Fatalf("tool resolver calls = %d, want 1", tools.calls)
	}
}

func TestRuntimeRunnerResolverClosesDuplicateCachedRunner(t *testing.T) {
	factory := newBlockingRunnerFactory()
	resolver, err := NewRuntimeRunnerResolver(testModelResolver{}, testSessionResolver{})
	if err != nil {
		t.Fatalf("new runtime runner resolver: %v", err)
	}
	resolver.newRunner = factory.New

	results := make(chan frameworkrunner.Runner, 2)
	errs := make(chan error, 2)
	for range 2 {
		go func() {
			resolved, resolveErr := resolver.ResolveRunner(context.Background(), testMemoryExecution("user-a"))
			results <- resolved
			errs <- resolveErr
		}()
	}
	select {
	case <-factory.started:
	case <-time.After(time.Second):
		t.Fatal("runner factory did not receive concurrent builds")
	}
	close(factory.release)
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatalf("resolve runner: %v", err)
		}
		if <-results == nil {
			t.Fatal("resolve runner returned nil")
		}
	}

	factory.mu.Lock()
	created := append([]*closeTrackingRunner(nil), factory.runners...)
	factory.mu.Unlock()
	if len(created) != 2 {
		t.Fatalf("created runners = %d, want 2", len(created))
	}
	closed := 0
	for _, runner := range created {
		if runner.closes.Load() == 1 {
			closed++
		}
	}
	if closed != 1 {
		t.Fatalf("closed duplicate runners = %d, want 1", closed)
	}
	if err := resolver.Close(); err != nil {
		t.Fatalf("close resolver: %v", err)
	}
	for _, runner := range created {
		if runner.closes.Load() != 1 {
			t.Fatalf("runner close count = %d, want 1", runner.closes.Load())
		}
	}
}

func TestRuntimeRunnerResolverDoesNotCacheMemoryRunners(t *testing.T) {
	ingestors := &testIngestorResolver{}
	resolver, err := NewRuntimeRunnerResolver(
		testModelResolver{},
		testSessionResolver{},
		WithSessionIngestorResolver(ingestors),
	)
	if err != nil {
		t.Fatalf("new runtime runner resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	first, err := resolver.ResolveRunner(context.Background(), testMemoryExecution("user-a"))
	if err != nil {
		t.Fatalf("resolve first runner: %v", err)
	}
	second, err := resolver.ResolveRunner(context.Background(), testMemoryExecution("user-b"))
	if err != nil {
		t.Fatalf("resolve second runner: %v", err)
	}
	if first == second {
		t.Fatal("memory-enabled executions reused a runner")
	}
	if len(ingestors.users) != 2 || ingestors.users[0] != "user-a" || ingestors.users[1] != "user-b" {
		t.Fatalf("ingestor users = %#v", ingestors.users)
	}
	if err := resolver.ReleaseRunner(first); err != nil {
		t.Fatalf("release first runner: %v", err)
	}
	if err := resolver.ReleaseRunner(second); err != nil {
		t.Fatalf("release second runner: %v", err)
	}
	if len(resolver.ephemeral) != 0 {
		t.Fatalf("ephemeral runners = %d", len(resolver.ephemeral))
	}
}

func TestRuntimeRunnerResolverDoesNotCacheArtifactRunners(t *testing.T) {
	artifacts := &testArtifactResolver{}
	resolver, err := NewRuntimeRunnerResolver(
		testModelResolver{},
		testSessionResolver{},
		WithArtifactResolver(artifacts),
	)
	if err != nil {
		t.Fatalf("new runtime runner resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })
	exec := testMemoryExecution("user-a")
	exec.Config.BackendConfig.Memory = tenant.BackendRef{}
	exec.Config.BackendConfig.Artifact = tenant.BackendRef{
		Kind: tenant.BackendObject, Provider: "cos", Name: "artifacts",
	}
	first, err := resolver.ResolveRunner(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve first runner: %v", err)
	}
	second, err := resolver.ResolveRunner(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve second runner: %v", err)
	}
	if first == second {
		t.Fatal("artifact-enabled executions reused a runner")
	}
	if artifacts.calls != 2 {
		t.Fatalf("artifact resolver calls = %d, want 2", artifacts.calls)
	}
	if err := resolver.ReleaseRunner(first); err != nil {
		t.Fatalf("release first runner: %v", err)
	}
	if err := resolver.ReleaseRunner(second); err != nil {
		t.Fatalf("release second runner: %v", err)
	}
}

type testTool struct{ name string }

func (t testTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{Name: t.name}
}

type testToolResolver struct {
	tools []frameworktool.Tool
	calls int
}

func (r *testToolResolver) ResolveTools(context.Context, worker.Execution) ([]frameworktool.Tool, error) {
	r.calls++
	return r.tools, nil
}

type blockingRunnerFactory struct {
	started     chan struct{}
	release     chan struct{}
	startedOnce sync.Once
	mu          sync.Mutex
	runners     []*closeTrackingRunner
}

func newBlockingRunnerFactory() *blockingRunnerFactory {
	return &blockingRunnerFactory{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (f *blockingRunnerFactory) New(_ string, _ frameworkagent.Agent, _ ...frameworkrunner.Option) frameworkrunner.Runner {
	runner := &closeTrackingRunner{}
	f.mu.Lock()
	f.runners = append(f.runners, runner)
	if len(f.runners) == 2 {
		f.startedOnce.Do(func() { close(f.started) })
	}
	f.mu.Unlock()
	<-f.release
	return runner
}

type closeTrackingRunner struct {
	closes atomic.Int32
}

func (r *closeTrackingRunner) Run(
	context.Context,
	string,
	string,
	frameworkmodel.Message,
	...frameworkagent.RunOption,
) (<-chan *frameworkevent.Event, error) {
	return nil, nil
}

func (r *closeTrackingRunner) Close() error {
	r.closes.Add(1)
	return nil
}

type testModelResolver struct{}

func (testModelResolver) ResolveModel(context.Context, worker.Execution) (ModelRuntime, error) {
	return ModelRuntime{Model: openai.New("gpt-test", openai.WithAPIKey("test-key"))}, nil
}

type testSessionResolver struct{}

func (testSessionResolver) ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error) {
	return noop.NewService(), nil
}

type testIngestorResolver struct {
	users []string
}

func (r *testIngestorResolver) ResolveSessionIngestor(
	_ context.Context,
	exec worker.Execution,
) (frameworksession.Ingestor, error) {
	r.users = append(r.users, exec.Tenant.UserID)
	return testIngestor{}, nil
}

func (*testIngestorResolver) RequiresPerExecutionRunner(worker.Execution) bool {
	return true
}

type testIngestor struct{}

func (testIngestor) IngestSession(
	context.Context,
	*frameworksession.Session,
	...frameworksession.IngestOption,
) error {
	return nil
}

type testArtifactResolver struct {
	calls int
}

func (r *testArtifactResolver) ResolveArtifact(context.Context, worker.Execution) (frameworkartifact.Service, error) {
	r.calls++
	return artifactmemory.NewService(), nil
}

func testMemoryExecution(userID string) worker.Execution {
	return worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID:      "tenant-a",
			AppID:         "app-a",
			ConfigVersion: "v1",
			UserID:        userID,
		},
		Config: tenant.AppConfig{
			TenantID: "tenant-a",
			AppID:    "app-a",
			Version:  "v1",
			Model: tenant.ModelConfig{
				Provider:  "openai",
				Model:     "gpt-test",
				APIKeyRef: tenant.SecretRef{Name: "model-key"},
			},
			BackendConfig: tenant.BackendConfig{
				Name: "backends",
				Session: tenant.BackendRef{
					Kind: tenant.BackendSQL,
					Name: "sessions",
				},
				Memory: tenant.BackendRef{
					Kind:      tenant.BackendExternal,
					Provider:  "tencentdb",
					Name:      "memory",
					SecretRef: tenant.SecretRef{Name: "memory-key"},
				},
			},
		},
	}
}
