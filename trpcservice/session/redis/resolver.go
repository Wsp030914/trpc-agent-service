// Package redis wires tenant-scoped Redis Session services.
package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// SessionResolver caches framework Redis Session services selected by immutable
// application configuration. Its provider keeps event persistence synchronous.
type SessionResolver struct {
	secrets    platformsecret.SecretProvider
	defaultURL string
	mu         sync.Mutex
	closed     bool
	services   map[string]frameworksession.Service
}

// NewSessionResolver creates a Redis session resolver backed by a scoped
// secret provider.
func NewSessionResolver(secrets platformsecret.SecretProvider, defaultURLs ...string) (*SessionResolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if len(defaultURLs) > 1 {
		return nil, errors.New("only one default redis url is supported")
	}
	defaultURL := ""
	if len(defaultURLs) == 1 {
		defaultURL = strings.TrimSpace(defaultURLs[0])
	}
	return &SessionResolver{secrets: secrets, defaultURL: defaultURL, services: make(map[string]frameworksession.Service)}, nil
}

// ResolveSession returns a framework Redis Session service for one execution.
func (r *SessionResolver) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil || r.secrets == nil {
		return nil, errors.New("redis session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Session
	if ref.Kind != tenant.BackendRedis || ref.Provider != "redis" {
		return nil, fmt.Errorf("session backend %q must use redis provider", ref.Name)
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
	url := r.defaultURL
	if ref.SecretRef != (tenant.SecretRef{}) {
		var err error
		url, err = r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), ref.SecretRef)
		if err != nil {
			return nil, fmt.Errorf("resolve session url: %w", err)
		}
	}
	if strings.TrimSpace(url) == "" {
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
	ref := exec.Config.BackendConfig.Session
	parts := []string{exec.Tenant.ConfigVersion, exec.Config.BackendConfig.Name, ref.Provider, ref.Name}
	if ref.SecretRef != (tenant.SecretRef{}) {
		parts = append(parts, ref.SecretRef.Name, ref.SecretRef.Version)
	}
	return exec.Tenant.Scope().Key("session-service", parts...)
}
