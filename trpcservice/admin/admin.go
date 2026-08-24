// Package admin defines the control-plane boundary for tenant configuration.
package admin

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// API validates tenant application configuration before publishing it.
type API struct {
	Bindings config.BindingResolver
}

// ValidateAppConfig checks app config fields and cross-resource references.
func (a API) ValidateAppConfig(ctx context.Context, cfg tenant.AppConfig) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := config.ValidateAppConfigBindings(ctx, cfg, a.Bindings); err != nil {
		return fmt.Errorf("channel bindings: %w", err)
	}
	return nil
}
