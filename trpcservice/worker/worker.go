// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Execution is the prepared context for running one tenant-scoped job.
type Execution struct {
	RequestID    string
	TenantSource gateway.TenantSource
	Tenant       tenant.RuntimeContext
	Config       tenant.AppConfig
	Backend      tenant.BackendProfile
	PartitionKey string
}

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Config  config.Resolver
	Storage storage.Resolver
}

// Prepare validates a job and resolves the tenant backend profile.
func (w Worker) Prepare(ctx context.Context, job gateway.Job) (Execution, error) {
	if err := job.Validate(); err != nil {
		return Execution{}, err
	}
	partitionKey, err := job.PartitionKey()
	if err != nil {
		return Execution{}, err
	}
	if w.Config == nil {
		return Execution{}, errors.New("config resolver is required")
	}
	cfg, err := w.Config.ResolveAppConfig(
		ctx,
		job.Tenant.TenantID,
		job.Tenant.AppID,
		job.Tenant.ConfigVersion,
	)
	if err != nil {
		return Execution{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Execution{}, fmt.Errorf("app config: %w", err)
	}
	if cfg.TenantID != job.Tenant.TenantID ||
		cfg.AppID != job.Tenant.AppID ||
		cfg.Version != job.Tenant.ConfigVersion {
		return Execution{}, errors.New("resolved app config does not match job scope")
	}
	if w.Storage == nil {
		return Execution{}, errors.New("storage resolver is required")
	}
	backend, err := w.Storage.ResolveBackend(ctx, job.Tenant)
	if err != nil {
		return Execution{}, err
	}
	if err := backend.Validate(); err != nil {
		return Execution{}, fmt.Errorf("backend profile: %w", err)
	}
	if !sameBackendProfile(cfg.Backend, backend) {
		return Execution{}, errors.New("resolved backend profile does not match app config")
	}
	return Execution{
		RequestID:    job.RequestID,
		TenantSource: job.TenantSource,
		Tenant:       job.Tenant,
		Config:       cfg,
		Backend:      backend,
		PartitionKey: partitionKey,
	}, nil
}

func sameBackendProfile(a, b tenant.BackendProfile) bool {
	return a.Name == b.Name &&
		sameBackendRef(a.Session, b.Session) &&
		sameBackendRef(a.Memory, b.Memory) &&
		sameBackendRef(a.Knowledge, b.Knowledge) &&
		sameBackendRef(a.Artifact, b.Artifact) &&
		sameBackendRef(a.Audit, b.Audit)
}

func sameBackendRef(a, b tenant.BackendRef) bool {
	if a.Kind != b.Kind || a.Name != b.Name || a.DSNRef != b.DSNRef {
		return false
	}
	return sameStringMap(a.Options, b.Options)
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		other, ok := b[key]
		if !ok || other != value {
			return false
		}
	}
	return true
}
