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

// Router selects a Session resolver by backend provider.
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
// registry can resolve ref.
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
