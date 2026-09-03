package runtime

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestToolCatalogAcceptsRegisteredTool(t *testing.T) {
	catalog := NewToolCatalog()
	policy := tenant.ToolPolicy{VisibleTools: []string{"todo_write"}}
	exec := worker.Execution{}
	exec.Config.Tools = policy

	tools, err := catalog.ResolveTools(context.Background(), exec)
	if err != nil || len(tools) != 1 || tools[0].Declaration().Name != "todo_write" {
		t.Fatalf("ResolveTools() = %#v, error = %v, want todo_write", tools, err)
	}
	if err := catalog.ValidateToolPolicy(context.Background(), policy); err != nil {
		t.Fatalf("ValidateToolPolicy() error = %v", err)
	}
}

func TestToolCatalogRejectsUnknownTool(t *testing.T) {
	catalog := NewToolCatalog()
	policy := tenant.ToolPolicy{VisibleTools: []string{"unknown"}}
	if err := catalog.ValidateToolPolicy(context.Background(), policy); err == nil ||
		!strings.Contains(err.Error(), "unsupported runtime tool") {
		t.Fatalf("ValidateToolPolicy() error = %v, want unsupported-tool rejection", err)
	}
}
