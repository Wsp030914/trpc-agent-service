// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrToolNotVisible reports that a tool cannot be exposed to a tenant app.
var ErrToolNotVisible = errors.New("tool is not visible")

// ErrToolNotExecutable reports that a tool cannot be executed by a tenant app.
var ErrToolNotExecutable = errors.New("tool is not executable")

// Declaration is the platform metadata needed before exposing a tool.
type Declaration struct {
	Name string
}

// Validate checks that the tool declaration can be matched by policy.
func (d Declaration) Validate() error {
	if d.Name == "" {
		return errors.New("tool name is required")
	}
	return nil
}

// VisibleDeclarations filters tools before they are exposed to a model.
func VisibleDeclarations(policy tenant.ToolPolicy, tools []Declaration) ([]Declaration, error) {
	visible := make([]Declaration, 0, len(tools))
	for i, tool := range tools {
		if err := tool.Validate(); err != nil {
			return nil, fmt.Errorf("tool %d: %w", i, err)
		}
		if !policy.CanView(tool.Name) {
			continue
		}
		visible = append(visible, tool)
	}
	return visible, nil
}

// AuthorizeVisibility checks the tenant tool policy before exposing a tool.
func AuthorizeVisibility(policy tenant.ToolPolicy, name string) error {
	if name == "" {
		return errors.New("tool name is required")
	}
	if !policy.CanView(name) {
		return fmt.Errorf("%w: %s", ErrToolNotVisible, name)
	}
	return nil
}

// AuthorizeExecution checks the tenant tool policy immediately before a tool run.
func AuthorizeExecution(policy tenant.ToolPolicy, name string) error {
	if name == "" {
		return errors.New("tool name is required")
	}
	if !policy.CanExecute(name) {
		return fmt.Errorf("%w: %s", ErrToolNotExecutable, name)
	}
	return nil
}
