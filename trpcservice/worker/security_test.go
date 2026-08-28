package worker

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestToolPermissionPolicyDeniesDisallowedToolBeforeCustomAuthorizer(t *testing.T) {
	authorizer := &recordingToolAuthorizer{}
	w := Worker{ToolAuthorizer: authorizer}
	exec := securityTestExecution()
	exec.Config.Tools = tenant.ToolPolicy{ExecutableTools: []string{"safe"}}

	decision, err := w.toolPermissionPolicy(exec).CheckToolPermission(context.Background(), &frameworktool.PermissionRequest{
		ToolName: "dangerous",
	})
	if err != nil {
		t.Fatalf("check tool permission: %v", err)
	}
	if decision.Action != frameworktool.PermissionActionDeny {
		t.Fatalf("decision = %q, want deny", decision.Action)
	}
	if authorizer.calls != 0 {
		t.Fatalf("custom authorizer calls = %d, want 0", authorizer.calls)
	}
}

func TestToolPermissionPolicyRevalidatesAllowedToolAtExecution(t *testing.T) {
	authorizer := &recordingToolAuthorizer{decision: frameworktool.AskPermission("review")}
	w := Worker{ToolAuthorizer: authorizer}
	exec := securityTestExecution()
	exec.Config.Tools = tenant.ToolPolicy{ExecutableTools: []string{"safe"}}

	decision, err := w.toolPermissionPolicy(exec).CheckToolPermission(context.Background(), &frameworktool.PermissionRequest{
		ToolName:  "safe",
		Arguments: []byte(`{"secret":"not logged"}`),
	})
	if err != nil {
		t.Fatalf("check tool permission: %v", err)
	}
	if decision.Action != frameworktool.PermissionActionAsk || authorizer.calls != 1 {
		t.Fatalf("decision = %q, calls = %d", decision.Action, authorizer.calls)
	}
}

func TestWorkerSpanAttributesExcludeUserAndSession(t *testing.T) {
	exec := securityTestExecution()
	for _, attr := range workerSpanAttributes(exec) {
		if attr.Key == "user_id" || attr.Key == "session_id" {
			t.Fatalf("unsafe span attribute %q", attr.Key)
		}
	}
}

func TestAuditEventRejectsArbitraryErrorText(t *testing.T) {
	err := (AuditEvent{
		Type:      AuditEventExecutionFailed,
		ErrorType: AuditErrorType("model api key abc123"),
	}).Validate()
	if err == nil {
		t.Fatal("audit event accepted arbitrary error text")
	}
}

func securityTestExecution() Execution {
	return Execution{
		RequestID: "request-1",
		Tenant: tenant.RuntimeContext{
			TenantID: "tenant-a", AppID: "app-a", ConfigVersion: "v1",
			SessionID: "session-1", SessionPrincipalID: "principal-1", UserID: "user-1", TraceID: "trace-1",
		},
		Config: tenant.AppConfig{
			TenantID: "tenant-a", AppID: "app-a", Version: "v1",
			Model: tenant.ModelConfig{Provider: "openai", Model: "gpt-test", APIKeyRef: tenant.SecretRef{Name: "model-key"}},
		},
	}
}

type recordingToolAuthorizer struct {
	calls    int
	decision frameworktool.PermissionDecision
}

func (a *recordingToolAuthorizer) CheckToolPermission(
	_ context.Context,
	_ Execution,
	_ *frameworktool.PermissionRequest,
) (frameworktool.PermissionDecision, error) {
	a.calls++
	return a.decision, nil
}
