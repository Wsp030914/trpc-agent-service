// Package secret resolves tenant-scoped secret references.
package secret

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// SecretProvider resolves a secret only after the worker has established the
// tenant application scope. Implementations must not persist or log values.
type SecretProvider interface {
	ResolveSecret(ctx context.Context, scope tenant.Scope, ref tenant.SecretRef) (string, error)
}

// SecretModelAPIKeyResolver adapts a scoped SecretProvider for an OpenAI model
// resolver. The model configuration selects the exact secret reference.
type SecretModelAPIKeyResolver struct {
	provider SecretProvider
}

// NewSecretModelAPIKeyResolver creates a model API key resolver backed by a
// scoped SecretProvider.
func NewSecretModelAPIKeyResolver(provider SecretProvider) (*SecretModelAPIKeyResolver, error) {
	if provider == nil {
		return nil, errors.New("secret provider is required")
	}
	return &SecretModelAPIKeyResolver{provider: provider}, nil
}

// ResolveModelAPIKey resolves only the API key reference selected by the
// immutable model configuration.
func (r *SecretModelAPIKeyResolver) ResolveModelAPIKey(ctx context.Context, exec worker.Execution) (string, error) {
	if r == nil || r.provider == nil {
		return "", errors.New("secret model api key resolver is not initialized")
	}
	if err := exec.Tenant.Scope().Validate(); err != nil {
		return "", fmt.Errorf("execution scope: %w", err)
	}
	ref := exec.Config.Model.APIKeyRef
	if err := ref.Validate(); err != nil {
		return "", fmt.Errorf("model api key ref: %w", err)
	}
	value, err := r.provider.ResolveSecret(ctx, exec.Tenant.Scope(), ref)
	if err != nil {
		return "", fmt.Errorf("resolve model api key: %w", err)
	}
	if value == "" {
		return "", errors.New("model api key is required")
	}
	return value, nil
}
