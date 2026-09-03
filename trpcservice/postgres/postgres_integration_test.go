package postgres_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const postgresTestDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"

var postgresTestDSN = flag.String(
	"postgres-test-dsn",
	os.Getenv(postgresTestDSNEnv),
	"dedicated PostgreSQL integration database DSN",
)

func TestPostgresMigrationFromEmptySchema(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS platform CASCADE; DROP SCHEMA IF EXISTS agent CASCADE`); err != nil {
		t.Fatalf("reset schemas: %v", err)
	}
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}

	results := make(chan error, 2)
	var group sync.WaitGroup
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			results <- store.Migrate(ctx)
		}()
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent migrate: %v", err)
		}
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("idempotent migrate: %v", err)
	}

	var sessionSchemaExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regnamespace('agent') IS NOT NULL`).Scan(&sessionSchemaExists); err != nil {
		t.Fatalf("check session schema: %v", err)
	}
	if sessionSchemaExists {
		t.Fatal("platform migration created agent session schema")
	}
	for _, table := range []string{
		"tenant",
		"agent_app",
		"app_config_version",
		"api_credential",
		"channel_binding",
		"session_lane",
		"execution",
		"dispatch_outbox",
		"execution_event",
		"data_migration",
		"artifact",
		"knowledge_base",
		"knowledge_document",
		"knowledge_chunk",
		"channel_identity",
		"channel_conversation",
		"channel_inbox",
		"channel_recall_inbox",
		"reply_outbox",
		"audit_event",
	} {
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "platform."+table).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table platform.%s does not exist", table)
		}
	}
}

func TestPostgresPublishedRecordsKeepSafetyConstraints(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantID := fmt.Sprintf("constraints-%d", time.Now().UnixNano())
	appID := "support"
	config := integrationAppConfig("v1", "constraint-model")
	config.TenantID, config.AppID = tenantID, appID
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support", ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE platform.app_config_version
SET model_config = '{"provider":"other","model":"changed"}'::jsonb
WHERE tenant_id = $1 AND app_id = $2 AND version = $3`, tenantID, appID, config.Version); err == nil {
		t.Fatal("published app config update succeeded")
	} else {
		assertPostgresSQLState(t, err, "55000")
	}

	digest, err := auth.DigestAPIKey("tas_constraint_test_key_with_at_least_32_random_bytes")
	if err != nil {
		t.Fatalf("digest API key: %v", err)
	}
	credential := auth.Credential{
		ID: "credential-constraints", TenantID: tenantID, AppID: appID,
		KeyPrefix: "tas_con", Status: auth.CredentialActive,
	}
	if err := store.CreateCredential(ctx, digest, credential); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	if err := store.RevokeCredential(ctx, tenantID, appID, credential.ID); err != nil {
		t.Fatalf("revoke credential: %v", err)
	}
	revoked, err := store.ResolveAPIKey(ctx, digest)
	if err != nil {
		t.Fatalf("resolve revoked credential: %v", err)
	}
	if revoked.Status != auth.CredentialRevoked {
		t.Fatalf("revoked credential status = %q", revoked.Status)
	}
	if _, err := pool.Exec(ctx, `
UPDATE platform.api_credential
SET status = 'ACTIVE'
WHERE tenant_id = $1 AND app_id = $2 AND credential_id = $3`, tenantID, appID, credential.ID); err == nil {
		t.Fatal("reactivating revoked credential succeeded")
	} else {
		assertPostgresSQLState(t, err, "55000")
	}
}

func assertPostgresSQLState(t *testing.T, err error, want string) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != want {
		t.Fatalf("postgres SQLSTATE = %v, want %s", err, want)
	}
}

func openIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := *postgresTestDSN
	if dsn == "" {
		t.Skipf("%s is not set", postgresTestDSNEnv)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres test DSN: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open postgres test pool: %v", err)
	}
	t.Cleanup(pool.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping postgres test database: %v", err)
	}
	return pool
}

func integrationAppConfig(version, modelName string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  version,
		Model: tenant.ModelConfig{
			Provider: "openai",
			Model:    modelName,
			APIKeyRef: tenant.SecretRef{
				Name:    "model-key",
				Version: "1",
			},
		},
		Tools: tenant.ToolPolicy{
			VisibleTools:    []string{"search"},
			ExecutableTools: []string{"search"},
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-postgres",
				Options:  map[string]string{"schema": "agent"},
			},
		},
		SecretRefs: []tenant.SecretRef{{Name: "model-key", Version: "1"}},
	}
}
