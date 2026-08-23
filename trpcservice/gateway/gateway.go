// Package gateway defines the tenant-aware request ingress boundary.
package gateway

import (
	"context"
	"errors"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// TenantSource identifies the trusted boundary that supplied tenant routing.
type TenantSource string

const (
	// TenantSourceAuthenticatedClaims means tenant routing came from authenticated claims.
	TenantSourceAuthenticatedClaims TenantSource = "authenticated_claims"
	// TenantSourceVerifiedChannelBinding means tenant routing came from a verified channel binding.
	TenantSourceVerifiedChannelBinding TenantSource = "verified_channel_binding"
)

// Message is the normalized user input passed from gateway to workers.
type Message struct {
	Text         string
	ArtifactRefs []string
}

// TenantResolver supplies tenant routing from an authentication or verification boundary.
// Implementations must not derive tenant identity from unverified external payload fields.
type TenantResolver interface {
	ResolveTenant(ctx context.Context) (tenant.RuntimeContext, TenantSource, error)
}

// Request is a gateway input whose tenant routing is supplied by a trusted resolver.
type Request struct {
	RequestID string
	Tenant    TenantResolver
	Message   Message
}

// Job is the tenant-scoped work item consumed by workers.
type Job struct {
	RequestID    string
	TenantSource TenantSource
	Tenant       tenant.RuntimeContext
	Message      Message
}

// Validate checks the trusted routing fields required by workers.
func (j Job) Validate() error {
	if j.RequestID == "" {
		return errors.New("request_id is required")
	}
	if !validTenantSource(j.TenantSource) {
		return errors.New("tenant source is invalid")
	}
	if j.TenantSource == TenantSourceVerifiedChannelBinding {
		if j.Tenant.Channel == "" {
			return errors.New("channel is required for verified channel binding")
		}
		if j.Tenant.BindingID == "" {
			return errors.New("binding_id is required for verified channel binding")
		}
	}
	return j.Tenant.Validate()
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
	job, err := NewJob(ctx, req)
	if err != nil {
		return Job{}, err
	}
	if g.Jobs != nil {
		if err := g.Jobs.Enqueue(ctx, job.clone()); err != nil {
			return Job{}, err
		}
	}
	return job, nil
}

// NewJob resolves trusted tenant routing and creates a worker job.
func NewJob(ctx context.Context, req Request) (Job, error) {
	if req.Tenant == nil {
		return Job{}, errors.New("tenant resolver is required")
	}
	tc, source, err := req.Tenant.ResolveTenant(ctx)
	if err != nil {
		return Job{}, err
	}
	job := Job{
		RequestID:    req.RequestID,
		TenantSource: source,
		Tenant:       tc,
		Message: Message{
			Text:         req.Message.Text,
			ArtifactRefs: cloneStrings(req.Message.ArtifactRefs),
		},
	}
	if err := job.Validate(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func validTenantSource(source TenantSource) bool {
	return source == TenantSourceAuthenticatedClaims || source == TenantSourceVerifiedChannelBinding
}

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}

func (j Job) clone() Job {
	cloned := j
	cloned.Message.ArtifactRefs = cloneStrings(j.Message.ArtifactRefs)
	return cloned
}
