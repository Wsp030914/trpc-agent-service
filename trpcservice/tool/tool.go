// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrToolNotVisible reports that a tool cannot be exposed to a tenant app.
var ErrToolNotVisible = errors.New("tool is not visible")

// ErrToolNotExecutable reports that a tool cannot be executed by a tenant app.
var ErrToolNotExecutable = errors.New("tool is not executable")

type idempotencyKeyContextKey struct{}

// StableIdempotencyKey derives a deterministic key for a platform-controlled
// tool side effect. The raw argument bytes are never part of logs or storage;
// only their digest participates in the key.
func StableIdempotencyKey(tenantID, appID, requestID, toolCallID, toolName string, args []byte) string {
	argsDigest := sha256.Sum256(args)
	h := sha256.New()
	for _, value := range []string{tenantID, appID, requestID, toolCallID, toolName, hex.EncodeToString(argsDigest[:])} {
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// WithIdempotencyKey makes a stable side-effect key available to a tool via
// its execution context.
func WithIdempotencyKey(ctx context.Context, key string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, idempotencyKeyContextKey{}, key)
}

// IdempotencyKey returns the platform key attached to the current tool call.
func IdempotencyKey(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	key, _ := ctx.Value(idempotencyKeyContextKey{}).(string)
	return key
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
