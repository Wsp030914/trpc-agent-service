// Package redis wires tenant-scoped Redis Session services.
package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// SessionURLResolver resolves a Redis URL for a configured Session backend.
type SessionURLResolver interface {
	ResolveSessionURL(context.Context, storage.Handle) (string, error)
}

// SessionResolver caches framework Redis Session services selected by immutable
// application configuration. Its provider keeps event persistence synchronous.
type SessionResolver struct {
	urls     SessionURLResolver
	mu       sync.Mutex
	closed   bool
	services map[string]frameworksession.Service
}

// NewSessionResolver creates a Redis Session resolver.
func NewSessionResolver(urls SessionURLResolver) (*SessionResolver, error) {
	if urls == nil {
		return nil, errors.New("session url resolver is required")
	}
	return &SessionResolver{urls: urls, services: make(map[string]frameworksession.Service)}, nil
}

// ResolveSession returns a framework Redis Session service for one execution.
func (r *SessionResolver) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil || r.urls == nil {
		return nil, errors.New("redis session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	handle := exec.Storage.Session
	if err := handle.Validate(exec.Tenant.Scope(), storage.CapabilitySession, exec.Config.BackendConfig.Session); err != nil {
		return nil, fmt.Errorf("session storage handle: %w", err)
	}
	if handle.Ref.Kind != tenant.BackendRedis || (handle.Ref.Provider != "" && handle.Ref.Provider != "redis") {
		return nil, fmt.Errorf("session backend %q must use redis provider", handle.Ref.Name)
	}
	key, err := sessionCacheKey(exec)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("redis session resolver is closed")
	}
	if service := r.services[key]; service != nil {
		return service, nil
	}
	url, err := r.urls.ResolveSessionURL(ctx, handle)
	if err != nil {
		return nil, fmt.Errorf("resolve session url: %w", err)
	}
	if url == "" {
		return nil, errors.New("session redis url is required")
	}
	service, err := redisprovider.NewService(redisprovider.WithRedisClientURL(url))
	if err != nil {
		return nil, fmt.Errorf("create redis session service: %w", err)
	}
	r.services[key] = service
	return service, nil
}

// Close closes cached Session services.
func (r *SessionResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := r.services
	r.services = nil
	r.mu.Unlock()
	var errs []error
	for _, service := range services {
		if err := service.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func sessionCacheKey(exec worker.Execution) (string, error) {
	ref := exec.Storage.Session.Ref
	parts := []string{exec.Tenant.ConfigVersion, exec.Config.BackendConfig.Name, ref.Provider, ref.Name}
	if ref.SecretRef != (tenant.SecretRef{}) {
		parts = append(parts, ref.SecretRef.Name, ref.SecretRef.Version)
	} else if ref.DSNRef != "" {
		parts = append(parts, ref.DSNRef)
	}
	return exec.Tenant.Scope().Key("session-service", parts...)
}
