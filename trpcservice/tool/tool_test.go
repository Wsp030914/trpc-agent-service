package tool_test

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

func TestAuthorizeExecutionChecksExecutablePolicy(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:    []string{"search", "delete"},
		ExecutableTools: []string{"search"},
	}

	if err := tool.AuthorizeExecution(policy, "search"); err != nil {
		t.Fatalf("authorize executable tool: %v", err)
	}
	if err := tool.AuthorizeExecution(policy, "delete"); !errors.Is(err, tool.ErrToolNotExecutable) {
		t.Fatalf("authorize non-executable tool error = %v, want ErrToolNotExecutable", err)
	}
}

func TestAuthorizeVisibilityChecksVisiblePolicy(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:    []string{"search"},
		ExecutableTools: []string{"search"},
	}

	if err := tool.AuthorizeVisibility(policy, "search"); err != nil {
		t.Fatalf("authorize visible tool: %v", err)
	}
	if err := tool.AuthorizeVisibility(policy, "delete"); !errors.Is(err, tool.ErrToolNotVisible) {
		t.Fatalf("authorize invisible tool error = %v, want ErrToolNotVisible", err)
	}
}
