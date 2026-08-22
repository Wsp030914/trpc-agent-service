// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Execution is the prepared context for running one tenant-scoped job.
type Execution struct {
	RequestID    string
	Tenant       tenant.RuntimeContext
	Backend      tenant.BackendProfile
	PartitionKey string
}

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Storage storage.Resolver
}

// Prepare validates a job and resolves the tenant backend profile.
func (w Worker) Prepare(ctx context.Context, job gateway.Job) (Execution, error) {
	if job.RequestID == "" {
		return Execution{}, errors.New("request_id is required")
	}
	if err := job.Tenant.Validate(); err != nil {
		return Execution{}, err
	}
	partitionKey, err := job.PartitionKey()
	if err != nil {
		return Execution{}, err
	}
	var backend tenant.BackendProfile
	if w.Storage != nil {
		backend, err = w.Storage.ResolveBackend(ctx, job.Tenant)
		if err != nil {
			return Execution{}, err
		}
	}
	return Execution{
		RequestID:    job.RequestID,
		Tenant:       job.Tenant,
		Backend:      backend,
		PartitionKey: partitionKey,
	}, nil
}
