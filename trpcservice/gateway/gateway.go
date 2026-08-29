// Package gateway defines the tenant-aware request ingress boundary.
package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"

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

var (
	// ErrAdmitterRequired means a Gateway has no atomic admission backend.
	ErrAdmitterRequired = errors.New("admitter is required")
	// ErrAdmissionIdentityRequired means the ingress did not provide an
	// identity that can be revalidated by the admission transaction.
	ErrAdmissionIdentityRequired = errors.New("admission identity resolver is required")
	// ErrIdempotencyConflict means one idempotency key was reused with another
	// normalized request payload.
	ErrIdempotencyConflict = errors.New("idempotency key conflicts with existing request")
	// ErrUnsupportedAdmissionSource means the selected admission backend does
	// not yet implement the request's trusted source type.
	ErrUnsupportedAdmissionSource = errors.New("admission source is unsupported")
	// ErrAdmissionDraining means backend migration is draining accepted work and
	// new requests must be retried after the advertised maintenance window.
	ErrAdmissionDraining = errors.New("request admission is draining for data migration")
)

// Message is the normalized user input passed from gateway to workers.
type Message struct {
	Text         string
	ArtifactRefs []string
}

// Validate checks whether Message is executable by the current runner boundary.
func (m Message) Validate() error {
	if m.Text == "" {
		return errors.New("message text is required")
	}
	if len(m.ArtifactRefs) > 0 {
		return errors.New("artifact refs are not supported by runner boundary")
	}
	return nil
}

// TenantResolver supplies tenant routing from an authentication or verification boundary.
// Implementations must not derive tenant identity from unverified external payload fields.
type TenantResolver interface {
	ResolveTenant(ctx context.Context) (tenant.RuntimeContext, TenantSource, error)
}

// CredentialDigest is a non-reversible API key digest carried to admission.
// It contains no raw credential material.
type CredentialDigest [sha256.Size]byte

// AdmissionIdentity is the trusted request identity that the admission
// transaction must revalidate against authoritative storage.
type AdmissionIdentity struct {
	Tenant           tenant.RuntimeContext
	Source           TenantSource
	SourceID         string
	CredentialDigest CredentialDigest
}

// Validate checks the trusted source and identity fields required for atomic
// admission. The config version is advisory and is re-read by the backend.
func (i AdmissionIdentity) Validate() error {
	if err := i.Tenant.Validate(); err != nil {
		return err
	}
	if !validTenantSource(i.Source) {
		return errors.New("tenant source is invalid")
	}
	if i.SourceID == "" {
		return errors.New("source_id is required")
	}
	if i.Source == TenantSourceAuthenticatedClaims && i.CredentialDigest == (CredentialDigest{}) {
		return errors.New("credential digest is required")
	}
	if i.Source == TenantSourceVerifiedChannelBinding {
		if i.Tenant.Channel == "" {
			return errors.New("channel is required for verified channel binding")
		}
		if i.Tenant.BindingID == "" {
			return errors.New("binding_id is required for verified channel binding")
		}
	}
	return nil
}

// AdmissionIdentityResolver exposes an authenticated identity to the Gateway
// without exposing raw credentials or trusting request payload tenant fields.
type AdmissionIdentityResolver interface {
	ResolveAdmissionIdentity(ctx context.Context) (AdmissionIdentity, error)
}

// Request is a gateway input whose tenant routing is supplied by a trusted resolver.
type Request struct {
	RequestID      string
	IdempotencyKey string
	Tenant         TenantResolver
	Message        Message
}

// AdmissionRequest is the normalized command submitted to the atomic
// admission backend.
type AdmissionRequest struct {
	RequestID      string
	IdempotencyKey string
	Identity       AdmissionIdentity
	Message        Message
}

// Validate checks the fields that must be stable before the admission
// transaction allocates a turn or writes an execution.
func (r AdmissionRequest) Validate() error {
	if r.RequestID == "" {
		return errors.New("request_id is required")
	}
	if r.IdempotencyKey == "" {
		return errors.New("idempotency_key is required")
	}
	if err := r.Identity.Validate(); err != nil {
		return fmt.Errorf("admission identity: %w", err)
	}
	if err := r.Message.Validate(); err != nil {
		return fmt.Errorf("message: %w", err)
	}
	return nil
}

// AdmissionResult describes the committed execution identity returned to an
// ingress after atomic admission. Replayed is true when an identical
// idempotent request already existed.
type AdmissionResult struct {
	RequestID     string
	ConfigVersion string
	TurnSeq       int64
	Replayed      bool
}

// Validate checks the result returned by an admission backend.
func (r AdmissionResult) Validate() error {
	if r.RequestID == "" {
		return errors.New("request_id is required")
	}
	if r.ConfigVersion == "" {
		return errors.New("config_version is required")
	}
	if r.TurnSeq <= 0 {
		return errors.New("turn_seq must be positive")
	}
	return nil
}

// Admitter atomically accepts a normalized request into the authoritative
// execution store.
type Admitter interface {
	Admit(ctx context.Context, request AdmissionRequest) (AdmissionResult, error)
}

// Option configures a Gateway.
type Option func(*Gateway)

// WithAdmitter sets the authoritative backend used for atomic request
// admission.
func WithAdmitter(admitter Admitter) Option {
	return func(g *Gateway) {
		g.Admitter = admitter
	}
}

// Gateway converts trusted requests into atomic admission commands.
type Gateway struct {
	Admitter Admitter
}

// New creates a Gateway with the provided options.
func New(opts ...Option) *Gateway {
	g := &Gateway{}
	for _, opt := range opts {
		opt(g)
	}
	return g
}

// Handle validates a request and submits it to the authoritative admission
// backend. It never performs a separate in-memory enqueue.
func (g Gateway) Handle(ctx context.Context, req Request) (AdmissionResult, error) {
	if g.Admitter == nil {
		return AdmissionResult{}, ErrAdmitterRequired
	}
	if req.Tenant == nil {
		return AdmissionResult{}, errors.New("tenant resolver is required")
	}
	identityResolver, ok := req.Tenant.(AdmissionIdentityResolver)
	if !ok {
		return AdmissionResult{}, ErrAdmissionIdentityRequired
	}
	identity, err := identityResolver.ResolveAdmissionIdentity(ctx)
	if err != nil {
		return AdmissionResult{}, err
	}
	admissionRequest := AdmissionRequest{
		RequestID:      req.RequestID,
		IdempotencyKey: req.IdempotencyKey,
		Identity:       identity,
		Message: Message{
			Text:         req.Message.Text,
			ArtifactRefs: slices.Clone(req.Message.ArtifactRefs),
		},
	}
	if err := admissionRequest.Validate(); err != nil {
		return AdmissionResult{}, err
	}
	result, err := g.Admitter.Admit(ctx, admissionRequest)
	if err != nil {
		return AdmissionResult{}, err
	}
	if err := result.Validate(); err != nil {
		return AdmissionResult{}, fmt.Errorf("admitter result: %w", err)
	}
	return result, nil
}

func validTenantSource(source TenantSource) bool {
	return source == TenantSourceAuthenticatedClaims || source == TenantSourceVerifiedChannelBinding
}
