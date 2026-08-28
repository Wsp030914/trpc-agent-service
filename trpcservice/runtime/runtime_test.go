package runtime

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
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

type testTool struct{ name string }

func (t testTool) Declaration() *frameworktool.Declaration {
	return &frameworktool.Declaration{Name: t.name}
}
