// Package storage defines tenant-scoped storage backend resolution.
package storage

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrInMemoryDisabled reports that an inmemory backend was selected outside
// an explicitly enabled local or test resolver.
var ErrInMemoryDisabled = errors.New("inmemory backend is disabled")

// Capability identifies one platform storage capability.
type Capability string

const (
	// CapabilitySession stores session events and state.
	CapabilitySession Capability = "session"
	// CapabilityMemory stores long-term memory records.
	CapabilityMemory Capability = "memory"
	// CapabilityKnowledge stores knowledge metadata and indexes.
	CapabilityKnowledge Capability = "knowledge"
	// CapabilityArtifact stores generated or uploaded artifacts.
	CapabilityArtifact Capability = "artifact"
	// CapabilityAudit stores tenant audit logs.
	CapabilityAudit Capability = "audit"
)

// Handle is a tenant-scoped reference to one configured backend.
type Handle struct {
	Scope      tenant.Scope
	Capability Capability
	Ref        tenant.BackendRef
}

// Key builds a tenant-scoped backend key for this capability.
func (h Handle) Key(parts ...string) (string, error) {
	if h.Capability == "" {
		return "", errors.New("storage capability is required")
	}
	return h.Scope.Key(string(h.Capability), parts...)
}

// IsZero reports whether the handle is not configured.
func (h Handle) IsZero() bool {
	return h.Scope == tenant.Scope{} && h.Capability == "" && h.Ref.IsZero()
}

// Validate checks that the handle matches the expected tenant scope, capability,
// and backend reference.
func (h Handle) Validate(scope tenant.Scope, capability Capability, ref tenant.BackendRef) error {
	if ref.IsZero() {
		if !h.IsZero() {
			return fmt.Errorf("%s backend is not configured but handle is present", capability)
		}
		return nil
	}
	if h.Scope != scope {
		return fmt.Errorf("%s backend scope does not match tenant app", capability)
	}
	if h.Capability != capability {
		return fmt.Errorf("%s backend capability does not match handle", capability)
	}
	if !sameBackendRef(h.Ref, ref) {
		return fmt.Errorf("%s backend ref does not match backend_config", capability)
	}
	return nil
}

// Handles groups the tenant-scoped backends used by one execution.
type Handles struct {
	Scope             tenant.Scope
	BackendConfigName string
	Session           Handle
	Memory            Handle
	Knowledge         Handle
	Artifact          Handle
	Audit             Handle
}

// Validate checks that handles match the runtime context and backend_config.
func (h Handles) Validate(tc tenant.RuntimeContext, backend tenant.BackendConfig) error {
	if err := tc.Validate(); err != nil {
		return err
	}
	if err := backend.Validate(); err != nil {
		return fmt.Errorf("backend_config: %w", err)
	}
	scope := tc.Scope()
	if h.Scope != scope {
		return errors.New("storage scope does not match tenant app")
	}
	if h.BackendConfigName != backend.Name {
		return errors.New("storage backend_config name does not match")
	}
	if err := h.Session.Validate(scope, CapabilitySession, backend.Session); err != nil {
		return err
	}
	if err := h.Memory.Validate(scope, CapabilityMemory, backend.Memory); err != nil {
		return err
	}
	if err := h.Knowledge.Validate(scope, CapabilityKnowledge, backend.Knowledge); err != nil {
		return err
	}
	if err := h.Artifact.Validate(scope, CapabilityArtifact, backend.Artifact); err != nil {
		return err
	}
	if err := h.Audit.Validate(scope, CapabilityAudit, backend.Audit); err != nil {
		return err
	}
	return nil
}

// Resolver resolves tenant backend_config into scoped storage handles.
type Resolver interface {
	Resolve(ctx context.Context, tc tenant.RuntimeContext, backend tenant.BackendConfig) (Handles, error)
}

// StaticResolver validates backend_config values and returns scoped handles.
//
// StaticResolver does not open network connections. It is intended for local
// wiring, tests, and first-stage platform assembly before concrete backend
// drivers are connected.
type StaticResolver struct {
	AllowInMemory bool
}

// Resolve validates a runtime context and backend_config, then returns
// tenant-scoped handles for each configured capability.
func (r StaticResolver) Resolve(
	ctx context.Context,
	tc tenant.RuntimeContext,
	backend tenant.BackendConfig,
) (Handles, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Handles{}, err
	}
	if err := tc.Validate(); err != nil {
		return Handles{}, err
	}
	if err := backend.Validate(); err != nil {
		return Handles{}, fmt.Errorf("backend_config: %w", err)
	}

	scope := tc.Scope()
	session, err := r.resolveHandle(scope, CapabilitySession, backend.Session)
	if err != nil {
		return Handles{}, err
	}
	memory, err := r.resolveOptionalHandle(scope, CapabilityMemory, backend.Memory)
	if err != nil {
		return Handles{}, err
	}
	knowledge, err := r.resolveOptionalHandle(scope, CapabilityKnowledge, backend.Knowledge)
	if err != nil {
		return Handles{}, err
	}
	artifact, err := r.resolveOptionalHandle(scope, CapabilityArtifact, backend.Artifact)
	if err != nil {
		return Handles{}, err
	}
	audit, err := r.resolveOptionalHandle(scope, CapabilityAudit, backend.Audit)
	if err != nil {
		return Handles{}, err
	}

	return Handles{
		Scope:             scope,
		BackendConfigName: backend.Name,
		Session:           session,
		Memory:            memory,
		Knowledge:         knowledge,
		Artifact:          artifact,
		Audit:             audit,
	}, nil
}

func (r StaticResolver) resolveOptionalHandle(
	scope tenant.Scope,
	capability Capability,
	ref tenant.BackendRef,
) (Handle, error) {
	if ref.IsZero() {
		return Handle{}, nil
	}
	return r.resolveHandle(scope, capability, ref)
}

func (r StaticResolver) resolveHandle(
	scope tenant.Scope,
	capability Capability,
	ref tenant.BackendRef,
) (Handle, error) {
	if err := ref.Validate(); err != nil {
		return Handle{}, fmt.Errorf("%s backend: %w", capability, err)
	}
	if ref.Kind == tenant.BackendInMemory && !r.AllowInMemory {
		return Handle{}, fmt.Errorf("%s backend %q: %w", capability, ref.Name, ErrInMemoryDisabled)
	}
	return Handle{
		Scope:      scope,
		Capability: capability,
		Ref:        ref.Clone(),
	}, nil
}

func sameBackendRef(a, b tenant.BackendRef) bool {
	return a.Kind == b.Kind &&
		a.Name == b.Name &&
		a.DSNRef == b.DSNRef &&
		reflect.DeepEqual(a.Options, b.Options)
}
