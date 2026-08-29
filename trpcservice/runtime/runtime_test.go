package runtime

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
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
