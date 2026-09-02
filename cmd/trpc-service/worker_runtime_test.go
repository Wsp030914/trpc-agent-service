package main

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestRuntimeToolResolverAcceptsRegisteredTool(t *testing.T) {
	policy := tenant.ToolPolicy{VisibleTools: []string{"todo_write"}}
	exec := worker.Execution{}
	exec.Config.Tools = policy

	tools, err := (runtimeToolResolver{}).ResolveTools(context.Background(), exec)
	if err != nil || len(tools) != 1 || tools[0].Declaration().Name != "todo_write" {
		t.Fatalf("ResolveTools() = %#v, error = %v, want todo_write", tools, err)
	}
	if err := (runtimeToolResolver{}).ValidateToolPolicy(context.Background(), policy); err != nil {
		t.Fatalf("ValidateToolPolicy() error = %v", err)
	}
}

func TestRuntimeToolResolverRejectsUnknownTool(t *testing.T) {
	policy := tenant.ToolPolicy{VisibleTools: []string{"unknown"}}
	if err := (runtimeToolResolver{}).ValidateToolPolicy(context.Background(), policy); err == nil ||
		!strings.Contains(err.Error(), "unsupported runtime tool") {
		t.Fatalf("ValidateToolPolicy() error = %v, want unsupported-tool rejection", err)
	}
}

func TestRuntimeToolResolverRequiresKnowledgeBackend(t *testing.T) {
	cfg := tenant.AppConfig{Tools: tenant.ToolPolicy{VisibleTools: []string{"search"}}}
	if err := (runtimeToolResolver{}).ValidateAppConfigTools(context.Background(), cfg); err == nil ||
		!strings.Contains(err.Error(), "requires a knowledge backend") {
		t.Fatalf("ValidateAppConfigTools() error = %v, want knowledge-backend rejection", err)
	}
}
