// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Resolver loads one immutable application config version in tenant scope.
type Resolver interface {
	// ResolveAppConfig returns a validated, caller-owned copy for an exact
	// tenant, application, and version match.
	ResolveAppConfig(ctx context.Context, tenantID, appID, version string) (tenant.AppConfig, error)
}

// StaticResolver stores immutable application configs for tests and local wiring.
// Its zero value is an empty resolver. Constructed resolvers are safe for concurrent reads.
type StaticResolver struct {
	configs map[configKey]tenant.AppConfig
}

type configKey struct {
	tenantID string
	appID    string
	version  string
}

// NewStaticResolver validates and copies application configs. It rejects duplicate scopes.
func NewStaticResolver(configs ...tenant.AppConfig) (*StaticResolver, error) {
	resolver := &StaticResolver{
		configs: make(map[configKey]tenant.AppConfig, len(configs)),
	}
	for i, cfg := range configs {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("app config %d: %w", i, err)
		}
		key := configKey{
			tenantID: cfg.TenantID,
			appID:    cfg.AppID,
			version:  cfg.Version,
		}
		if _, exists := resolver.configs[key]; exists {
			return nil, fmt.Errorf(
				"app config for tenant_id %q app_id %q version %q is duplicated",
				key.tenantID,
				key.appID,
				key.version,
			)
		}
		resolver.configs[key] = cfg.Clone()
	}
	return resolver, nil
}

// ResolveAppConfig returns a validated copy of one exact config version.
func (r *StaticResolver) ResolveAppConfig(
	_ context.Context,
	tenantID,
	appID,
	version string,
) (tenant.AppConfig, error) {
	if tenantID == "" {
		return tenant.AppConfig{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return tenant.AppConfig{}, errors.New("app_id is required")
	}
	if version == "" {
		return tenant.AppConfig{}, errors.New("config version is required")
	}

	key := configKey{tenantID: tenantID, appID: appID, version: version}
	cfg, ok := r.configs[key]
	if !ok {
		return tenant.AppConfig{}, fmt.Errorf(
			"app config not found for tenant_id %q app_id %q version %q",
			tenantID,
			appID,
			version,
		)
	}
	return cfg.Clone(), nil
}
