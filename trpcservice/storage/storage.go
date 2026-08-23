// Package storage defines tenant-scoped storage backend resolution.
package storage

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Resolver returns the backend profile selected for a tenant app version.
type Resolver interface {
	// ResolveBackend returns a validated, caller-owned backend profile for tc.
	ResolveBackend(ctx context.Context, tc tenant.RuntimeContext) (tenant.BackendProfile, error)
}

// StaticResolver is a local resolver for tests and first-phase wiring.
type StaticResolver struct {
	Backend tenant.BackendProfile
}

// ResolveBackend validates the runtime context and returns the configured backend.
func (r StaticResolver) ResolveBackend(_ context.Context, tc tenant.RuntimeContext) (tenant.BackendProfile, error) {
	if err := tc.Validate(); err != nil {
		return tenant.BackendProfile{}, err
	}
	if err := r.Backend.Validate(); err != nil {
		return tenant.BackendProfile{}, fmt.Errorf("backend profile: %w", err)
	}
	return r.Backend.Clone(), nil
}
