// Package cos resolves tenant-scoped COS artifact services.
package cos

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"mime"
	"sync"

	sharedcos "github.com/liuzengh/trpc-agent-service/internal/cosclient"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	cosclient "github.com/tencentyun/cos-go-sdk-v5"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	frameworkcos "trpc.group/trpc-go/trpc-agent-go/artifact/cos"
)

const (
	providerName = "cos"
)

// SecretProvider resolves a scoped COS credential bundle. Resolved values must
// not be logged or persisted by the caller.
type SecretProvider interface {
	ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error)
}

// EndpointResolver returns the operator-controlled COS bucket endpoint for a
// logical backend name. It must not use tenant-provided connection settings.
type EndpointResolver interface {
	ResolveCOSEndpoint(context.Context, string) (string, error)
}

// Resolver creates COS artifact services selected by immutable application
// configuration versions.
type Resolver struct {
	secrets   SecretProvider
	endpoints EndpointResolver

	mu       sync.Mutex
	closed   bool
	services map[string]artifact.Service
}

// NewResolver creates a COS artifact resolver.
func NewResolver(secrets SecretProvider, endpoints EndpointResolver) (*Resolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if endpoints == nil {
		return nil, errors.New("endpoint resolver is required")
	}
	return &Resolver{
		secrets:   secrets,
		endpoints: endpoints,
		services:  make(map[string]artifact.Service),
	}, nil
}

// ResolveArtifact returns the COS artifact service selected by exec. A nil
// service means the application has not configured an artifact backend.
func (r *Resolver) ResolveArtifact(ctx context.Context, exec worker.Execution) (artifact.Service, error) {
	if r == nil || r.secrets == nil || r.endpoints == nil {
		return nil, errors.New("cos artifact resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Artifact
	if ref.IsZero() {
		return nil, nil
	}
	if err := exec.Tenant.Validate(); err != nil {
		return nil, err
	}
	if err := exec.Storage.Artifact.Validate(exec.Tenant.Scope(), storage.CapabilityArtifact, ref); err != nil {
		return nil, fmt.Errorf("artifact storage handle: %w", err)
	}
	if err := ValidateBackend(ref); err != nil {
		return nil, err
	}
	scope := exec.Tenant.Scope()
	endpoint, err := r.endpoints.ResolveCOSEndpoint(ctx, ref.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve cos endpoint: %w", err)
	}
	if err := sharedcos.ValidateEndpoint(endpoint); err != nil {
		return nil, err
	}
	serviceKey, err := artifactServiceKey(scope, exec.Tenant.ConfigVersion, ref, endpoint)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("cos artifact resolver is closed")
	}
	if service := r.services[serviceKey]; service != nil {
		r.mu.Unlock()
		return service, nil
	}
	r.mu.Unlock()

	credential, err := r.secrets.ResolveSecret(ctx, scope, ref.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("resolve cos credentials: %w", err)
	}
	client, err := sharedcos.New(endpoint, credential)
	if err != nil {
		return nil, err
	}
	frameworkService, err := frameworkcos.NewService(
		ref.Name,
		endpoint,
		frameworkcos.WithClient(client),
	)
	if err != nil {
		return nil, fmt.Errorf("create cos artifact service: %w", err)
	}
	service := &versionedService{
		Service: frameworkService,
		client:  client,
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("cos artifact resolver is closed")
	}
	if existing := r.services[serviceKey]; existing != nil {
		return existing, nil
	}
	r.services[serviceKey] = service
	return service, nil
}

type versionedService struct {
	artifact.Service
	client *cosclient.Client
}

func (s *versionedService) SaveArtifactVersion(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
	version int,
	value *artifact.Artifact,
) error {
	if s == nil || s.Service == nil || s.client == nil {
		return errors.New("cos versioned artifact service is not initialized")
	}
	if value == nil {
		return errors.New("artifact is required")
	}
	keyer, ok := s.Service.(interface {
		ObjectKey(artifact.SessionInfo, string, int) (string, error)
	})
	if !ok {
		return errors.New("cos artifact service does not expose version object keys")
	}
	key, err := keyer.ObjectKey(info, filename, version)
	if err != nil {
		return fmt.Errorf("resolve cos artifact version key: %w", err)
	}
	_, err = s.client.Object.Put(ctx, key, bytes.NewReader(value.Data), &cosclient.ObjectPutOptions{
		ObjectPutHeaderOptions: &cosclient.ObjectPutHeaderOptions{
			ContentType: value.MimeType,
			ContentDisposition: mime.FormatMediaType("attachment", map[string]string{
				"filename": filename,
			}),
		},
	})
	if err != nil {
		return fmt.Errorf("upload cos artifact version: %w", err)
	}
	return nil
}

func (s *versionedService) DeleteArtifactVersion(
	ctx context.Context,
	info artifact.SessionInfo,
	filename string,
	version int,
) error {
	if s == nil || s.Service == nil || s.client == nil {
		return errors.New("cos versioned artifact service is not initialized")
	}
	keyer, ok := s.Service.(interface {
		ObjectKey(artifact.SessionInfo, string, int) (string, error)
	})
	if !ok {
		return errors.New("cos artifact service does not expose version object keys")
	}
	key, err := keyer.ObjectKey(info, filename, version)
	if err != nil {
		return fmt.Errorf("resolve cos artifact version key: %w", err)
	}
	_, err = s.client.Object.Delete(ctx, key)
	if err != nil && !cosclient.IsNotFoundError(err) {
		return fmt.Errorf("delete cos artifact version: %w", err)
	}
	return nil
}

// ValidateBackend checks that ref selects the COS artifact adapter.
func ValidateBackend(ref tenant.BackendRef) error {
	if ref.Kind != tenant.BackendObject {
		return errors.New("cos artifact backend kind must be object")
	}
	if ref.Provider != providerName {
		return fmt.Errorf("unsupported artifact provider %q", ref.Provider)
	}
	if ref.SecretRef == (tenant.SecretRef{}) {
		return errors.New("cos artifact backend secret_ref is required")
	}
	return nil
}

// Close releases cached service references. COS services do not own a
// closeable connection, so callers may safely call Close multiple times.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.services = nil
	return nil
}

func artifactServiceKey(scope tenant.Scope, version string, ref tenant.BackendRef, endpoint string) (string, error) {
	parts := []string{version, ref.Provider, ref.Name, ref.SecretRef.Name}
	if ref.SecretRef.Version != "" {
		parts = append(parts, ref.SecretRef.Version)
	}
	parts = append(parts, endpoint)
	return scope.Key("artifact", parts...)
}
