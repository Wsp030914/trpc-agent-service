package postgres

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestValidateSupportedDataMigration(t *testing.T) {
	base := tenant.BackendConfig{
		Session: tenant.BackendRef{
			Kind:     tenant.BackendRedis,
			Provider: "redis",
			Name:     "source-session",
		},
		Memory: tenant.BackendRef{
			Kind:     tenant.BackendExternal,
			Provider: "tencentdb",
			Name:     "memory",
		},
		Artifact: tenant.BackendRef{
			Kind:     tenant.BackendObject,
			Provider: "cos",
			Name:     "artifact",
		},
	}
	target := base.Clone()
	target.Session = tenant.BackendRef{
		Kind:     tenant.BackendSQL,
		Provider: "postgres",
		Name:     "target-session",
	}

	if err := validateSupportedDataMigration(base, target); err != nil {
		t.Fatalf("validate supported migration: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*tenant.BackendConfig, *tenant.BackendConfig)
		wantErr string
	}{
		{
			name: "memory change",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Memory.Name = "other-memory"
			},
			wantErr: "only supports session backend changes",
		},
		{
			name: "artifact change",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Artifact.Name = "other-artifact"
			},
			wantErr: "only supports session backend changes",
		},
		{
			name: "source is not redis",
			mutate: func(source, _ *tenant.BackendConfig) {
				source.Session.Kind = tenant.BackendSQL
			},
			wantErr: "source session backend must use redis",
		},
		{
			name: "target is not postgres",
			mutate: func(_, target *tenant.BackendConfig) {
				target.Session.Kind = tenant.BackendRedis
			},
			wantErr: "target session backend must use postgres",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source, destination := base.Clone(), target.Clone()
			tt.mutate(&source, &destination)
			err := validateSupportedDataMigration(source, destination)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validate migration error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}
