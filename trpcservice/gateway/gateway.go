// Package gateway defines the tenant-aware request ingress boundary.
package gateway

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Message is the normalized user input passed from gateway to workers.
type Message struct {
	Text         string
	ArtifactRefs []string
}

// Request is a trusted gateway input after authentication or channel binding.
type Request struct {
	RequestID string
	Tenant    tenant.RuntimeContext
	Message   Message
}

// Job is the tenant-scoped work item consumed by workers.
type Job struct {
	RequestID string
	Tenant    tenant.RuntimeContext
	Message   Message
}

// PartitionKey returns the key used to serialize work for one session.
func (j Job) PartitionKey() (string, error) {
	return j.Tenant.Scope().Key("session", j.Tenant.SessionID)
}

// Enqueuer accepts tenant-scoped jobs for later worker consumption.
type Enqueuer interface {
	Enqueue(ctx context.Context, job Job) error
}

// Gateway converts trusted requests into tenant-scoped jobs.
type Gateway struct {
	Jobs Enqueuer
}

// Handle validates a request, creates a job, and optionally enqueues it.
func (g Gateway) Handle(ctx context.Context, req Request) (Job, error) {
	job, err := NewJob(req)
	if err != nil {
		return Job{}, err
	}
	if g.Jobs != nil {
		if err := g.Jobs.Enqueue(ctx, job); err != nil {
			return Job{}, err
		}
	}
	return job, nil
}

// NewJob creates a worker job from one trusted gateway request.
func NewJob(req Request) (Job, error) {
	if req.RequestID == "" {
		return Job{}, errors.New("request_id is required")
	}
	if err := req.Tenant.Validate(); err != nil {
		return Job{}, err
	}
	return Job{
		RequestID: req.RequestID,
		Tenant:    req.Tenant,
		Message:   req.Message,
	}, nil
}
