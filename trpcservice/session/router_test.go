package session

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestValidateBackend(t *testing.T) {
	tests := []struct {
		name    string
		ref     tenant.BackendRef
		wantErr string
	}{
		{
			name: "legacy sql",
			ref:  tenant.BackendRef{Kind: tenant.BackendSQL, Name: "sessions"},
		},
		{
			name: "legacy redis",
			ref:  tenant.BackendRef{Kind: tenant.BackendRedis, Name: "sessions"},
		},
		{
			name: "explicit postgres",
			ref:  tenant.BackendRef{Kind: tenant.BackendSQL, Provider: postgresProvider, Name: "sessions"},
		},
		{
			name: "explicit redis",
			ref:  tenant.BackendRef{Kind: tenant.BackendRedis, Provider: redisProvider, Name: "sessions"},
		},
		{
			name:    "unsupported provider",
			ref:     tenant.BackendRef{Kind: tenant.BackendSQL, Provider: "mysql", Name: "sessions"},
			wantErr: `session provider "mysql" is not supported`,
		},
		{
			name:    "provider kind mismatch",
			ref:     tenant.BackendRef{Kind: tenant.BackendRedis, Provider: postgresProvider, Name: "sessions"},
			wantErr: "does not match",
		},
		{
			name:    "inmemory provider",
			ref:     tenant.BackendRef{Kind: tenant.BackendInMemory, Provider: "inmemory", Name: "sessions"},
			wantErr: `session provider "inmemory" is not supported`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateBackend(test.ref)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateBackend() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("ValidateBackend() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}
