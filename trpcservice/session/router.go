// Package session contains platform Session provider assembly.
package session

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

// Resolver resolves a framework Session service for a prepared execution.
type Resolver interface {
	ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error)
	Close() error
}

// Router selects a Session resolver by backend provider. Empty providers in
// configurations published before phase two retain their historic defaults.
type Router struct {
	resolvers map[string]Resolver
}

const (
	postgresProvider = "postgres"
	redisProvider    = "redis"
)

// NewRouter creates a Session resolver router from explicitly registered providers.
func NewRouter(resolvers map[string]Resolver) (*Router, error) {
	if len(resolvers) == 0 {
		return nil, errors.New("session resolvers are required")
	}
	cloned := make(map[string]Resolver, len(resolvers))
	for provider, resolver := range resolvers {
		if provider == "" || resolver == nil {
			return nil, errors.New("session provider and resolver are required")
		}
		if reflect.TypeOf(resolver).Kind() != reflect.Pointer {
			return nil, errors.New("session resolver must be a pointer")
		}
		cloned[provider] = resolver
	}
	return &Router{resolvers: cloned}, nil
}

// ValidateBackend checks whether the platform's built-in Session provider
// registry can resolve ref. An empty provider retains the SQL and Redis
// defaults used by older configuration versions.
func ValidateBackend(ref tenant.BackendRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	provider, err := sessionProvider(ref)
	if err != nil {
		return err
	}
	switch provider {
	case postgresProvider:
		if ref.Kind != tenant.BackendSQL {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, provider)
		}
	case redisProvider:
		if ref.Kind != tenant.BackendRedis {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, provider)
		}
	default:
		return fmt.Errorf("session provider %q is not supported", provider)
	}
	return nil
}

// ResolveSession selects the provider registered for the execution Session backend.
func (r *Router) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil {
		return nil, errors.New("session router is not initialized")
	}
	provider, err := sessionProvider(exec.Config.BackendConfig.Session)
	if err != nil {
		return nil, err
	}
	resolver := r.resolvers[provider]
	if resolver == nil {
		return nil, fmt.Errorf("session provider %q is not supported", provider)
	}
	return resolver.ResolveSession(ctx, exec)
}

// Close closes every resolver owned by Router.
func (r *Router) Close() error {
	if r == nil {
		return nil
	}
	closed := make(map[uintptr]struct{}, len(r.resolvers))
	var errs []error
	for _, resolver := range r.resolvers {
		value := reflect.ValueOf(resolver).Pointer()
		if _, ok := closed[value]; ok {
			continue
		}
		closed[value] = struct{}{}
		if err := resolver.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func sessionProvider(ref tenant.BackendRef) (string, error) {
	if ref.Provider != "" {
		return ref.Provider, nil
	}
	switch ref.Kind {
	case tenant.BackendSQL:
		return postgresProvider, nil
	case tenant.BackendRedis:
		return redisProvider, nil
	default:
		return "", fmt.Errorf("session backend kind %q has no legacy provider", ref.Kind)
	}
}
