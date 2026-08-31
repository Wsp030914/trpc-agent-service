package main

import (
	"context"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestNoToolsResolverRejectsConfiguredTools(t *testing.T) {
	policy := tenant.ToolPolicy{VisibleTools: []string{"search"}}
	exec := worker.Execution{}
	exec.Config.Tools = policy

	if _, err := (noToolsResolver{}).ResolveTools(context.Background(), exec); err == nil ||
		!strings.Contains(err.Error(), "configured tools are not supported") {
		t.Fatalf("ResolveTools() error = %v, want configured-tools rejection", err)
	}
	if err := (noToolsResolver{}).ValidateToolPolicy(context.Background(), policy); err == nil ||
		!strings.Contains(err.Error(), "configured tools are not supported") {
		t.Fatalf("ValidateToolPolicy() error = %v, want configured-tools rejection", err)
	}
}
