// Package session contains platform Session provider assembly.
package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

// Resolver resolves a framework Session service for a prepared execution.
type Resolver interface {
	ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error)
	Close() error
}

// Router selects a Session resolver by backend provider.
type Router struct {
	postgres Resolver
	redis    Resolver
}

const (
	postgresProvider = "postgres"
	redisProvider    = "redis"
)

// NewRouter creates the built-in PostgreSQL and Redis Session routes.
func NewRouter(postgres Resolver, redis Resolver) (*Router, error) {
	if postgres == nil || redis == nil {
		return nil, errors.New("postgres and redis session providers are required")
	}
	return &Router{postgres: postgres, redis: redis}, nil
}

// ValidateBackend checks whether the platform's built-in Session provider
// providers can resolve ref.
func ValidateBackend(ref tenant.BackendRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	switch ref.Provider {
	case postgresProvider:
		if ref.Kind != tenant.BackendSQL {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, ref.Provider)
		}
	case redisProvider:
		if ref.Kind != tenant.BackendRedis {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, ref.Provider)
		}
	default:
		return fmt.Errorf("session provider %q is not supported", ref.Provider)
	}
	return nil
}

// ResolveSession selects the provider registered for the execution Session backend.
func (r *Router) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil {
		return nil, errors.New("session router is not initialized")
	}
	provider := exec.Config.BackendConfig.Session.Provider
	var resolver Resolver
	switch provider {
	case postgresProvider:
		resolver = r.postgres
	case redisProvider:
		resolver = r.redis
	default:
		return nil, fmt.Errorf("session provider %q is not supported", provider)
	}
	if resolver == nil {
		return nil, fmt.Errorf("session provider %q is not configured", provider)
	}
	return resolver.ResolveSession(ctx, exec)
}

// Close closes both built-in providers owned by Router.
func (r *Router) Close() error {
	if r == nil {
		return nil
	}
	var errs []error
	if r.postgres != nil {
		errs = append(errs, r.postgres.Close())
	}
	if r.redis != nil {
		errs = append(errs, r.redis.Close())
	}
	return errors.Join(errs...)
}
