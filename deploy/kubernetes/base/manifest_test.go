package base

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func readManifest(t *testing.T, name ...string) string {
	t.Helper()
	path := filepath.Join(name...)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func TestWorkerAndChannelDoNotMountAdminSecret(t *testing.T) {
	for _, name := range []string{"worker-deployment.yaml", "channel-deployment.yaml"} {
		manifest := readManifest(t, name)
		if strings.Contains(manifest, "trpc-agent-service-admin-secrets") ||
			strings.Contains(manifest, "TRPC_AGENT_SERVICE_ADMIN_TOKEN") {
			t.Errorf("%s exposes the admin secret", name)
		}
	}
}

func TestGatewayUsesSeparateAdminSecret(t *testing.T) {
	manifest := readManifest(t, "gateway-deployment.yaml")
	if !strings.Contains(manifest, "name: trpc-agent-service-admin-secrets") ||
		!strings.Contains(manifest, "key: TRPC_AGENT_SERVICE_ADMIN_TOKEN") {
		t.Fatal("gateway does not reference the control-plane secret")
	}
	secretExample := readManifest(t, "..", "secret.example.yaml")
	adminStart := strings.Index(secretExample, "name: trpc-agent-service-admin-secrets")
	if adminStart < 0 || strings.Contains(secretExample[:adminStart], "TRPC_AGENT_SERVICE_ADMIN_TOKEN") {
		t.Fatal("admin token remains in the shared Secret example")
	}
}

func TestComposePassesSecretBackendConfigurationToAllAppRoles(t *testing.T) {
	compose := readManifest(t, "..", "..", "..", "compose.yaml")
	if got := strings.Count(compose, "      TRPC_AGENT_SERVICE_SECRET_BACKEND:"); got != 4 {
		t.Fatalf("secret backend configuration appears %d times, want four app containers", got)
	}
	for _, key := range []string{
		"TRPC_AGENT_SERVICE_SECRET_DIR:",
		"TRPC_AGENT_SERVICE_VAULT_ADDR:",
		"TRPC_AGENT_SERVICE_VAULT_MOUNT:",
		"TRPC_AGENT_SERVICE_VAULT_TOKEN:",
		"TRPC_AGENT_SERVICE_VAULT_NAMESPACE:",
	} {
		if got := strings.Count(compose, "      "+key); got != 4 {
			t.Errorf("%s appears %d times, want four app containers", key, got)
		}
	}
}
