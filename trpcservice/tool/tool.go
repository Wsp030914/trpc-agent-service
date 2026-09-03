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
