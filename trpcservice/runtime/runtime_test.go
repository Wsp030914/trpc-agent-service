package runtime

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/noop"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
	frameworktodo "trpc.group/trpc-go/trpc-agent-go/tool/todo"
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

func TestRuntimeBuildsFreshRunnerAndConfiguredTool(t *testing.T) {
	models, err := NewOpenAIModelResolver(testSecrets{}, nil)
	if err != nil {
		t.Fatalf("new model resolver: %v", err)
	}
	sessions, err := platformsession.NewRouter(testSessionProvider{}, testSessionProvider{})
	if err != nil {
		t.Fatalf("new session router: %v", err)
	}
	runtime, err := NewRuntime(models, sessions, nil, nil, nil, NewToolCatalog())
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	exec := testExecution()
	exec.Config.Tools = tenant.ToolPolicy{VisibleTools: []string{frameworktodo.DefaultToolName}}
	first, err := runtime.BuildRunner(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve first runner: %v", err)
	}
	second, err := runtime.BuildRunner(context.Background(), exec)
	if err != nil {
		t.Fatalf("resolve second runner: %v", err)
	}
	defer first.Close()
	defer second.Close()
	if first == second {
		t.Fatal("executions reused a runner")
	}
}

type testTool struct{ name string }

func (t testTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{Name: t.name}
}

type testSecrets struct{}

func (testSecrets) ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error) {
	return "test-model-key", nil
}

type testSessionProvider struct{}

func (testSessionProvider) ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error) {
	return noop.NewService(), nil
}

func (testSessionProvider) Close() error { return nil }

func testExecution() worker.Execution {
	return worker.Execution{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "app-a",
			ConfigVersion:      "v1",
			SessionPrincipalID: "user-a",
			SessionID:          "session-a",
			UserID:             "user-a",
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
				Session: tenant.BackendRef{Kind: tenant.BackendSQL, Provider: "postgres", Name: "sessions"},
			},
		},
		Message: tenantMessage("hello"),
	}
}

func tenantMessage(text string) gateway.Message {
	return gateway.Message{Text: text}
}
