//go:build integration

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

var (
	dataMigrationTestDSN = flag.String(
		"data-migration-test-dsn",
		os.Getenv("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"),
		"dedicated PostgreSQL integration database DSN",
	)
	dataMigrationTestURL = flag.String(
		"data-migration-test-url",
		os.Getenv("TRPC_AGENT_SERVICE_REDIS_TEST_URL"),
		"Redis integration URL",
	)
)

func TestWorkerRuntimeCompletesRedisPostgresMigration(t *testing.T) {
	if *dataMigrationTestDSN == "" || *dataMigrationTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN and TRPC_AGENT_SERVICE_REDIS_TEST_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("mig%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })

	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     tenantID,
		Name:   "Migration Integration",
		Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            tenantID,
		AppID:               appID,
		Name:                "Migration Integration",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}

	scope := tenant.Scope{TenantID: tenantID, AppID: appID}
	appName, err := scope.Key("runner")
	if err != nil {
		t.Fatalf("build session app name: %v", err)
	}
	key := session.Key{AppName: appName, UserID: "user-1", SessionID: "session-1"}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("insert session lane: %v", err)
	}
	source, err := redisprovider.NewService(redisprovider.WithRedisClientURL(*dataMigrationTestURL))
	if err != nil {
		t.Fatalf("create redis session service: %v", err)
	}
	t.Cleanup(func() {
		_ = source.DeleteSession(context.Background(), key)
		_ = source.Close()
	})
	sourceSession, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")})
	if err != nil {
		t.Fatalf("create source session: %v", err)
	}
	if err := appendMigrationTestEvents(ctx, source, sourceSession); err != nil {
		t.Fatalf("append source events: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	if _, err := store.BeginDataMigration(ctx, tenantID, appID, record.ID, "worker-previous", time.Now().Add(time.Minute), dataMigrationLease); err != nil {
		t.Fatalf("begin data migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.data_migration SET lease_until = clock_timestamp() - interval '1 second' WHERE migration_id = $1`, record.ID); err != nil {
		t.Fatalf("expire migration lease: %v", err)
	}

	redisClient, err := platformredis.NewClient(ctx, *dataMigrationTestURL)
	if err != nil {
		t.Fatalf("create platform redis client: %v", err)
	}
	t.Cleanup(func() { _ = redisClient.Close() })
	stream, err := platformredis.NewStream(redisClient, fmt.Sprintf("migration-test:%x", seed), "integration", time.Second)
	if err != nil {
		t.Fatalf("create redis stream: %v", err)
	}
	runtime, err := newWorkerRuntime(store, *dataMigrationTestDSN, *dataMigrationTestURL, "", redisClient, stream, "worker-migration", environmentReader(nil))
	if err != nil {
		t.Fatalf("new worker runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.close() })
	if err := runtime.runDataMigrationPass(ctx); err != nil {
		t.Fatalf("run data migration pass: %v", err)
	}

	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after migration: %v", err)
	}
	if app.ActiveConfigVersion != v2.Version {
		t.Fatalf("active config = %q, want %q", app.ActiveConfigVersion, v2.Version)
	}
	status := migration.Status("")
	if err := pool.QueryRow(ctx, `SELECT status FROM platform.data_migration WHERE migration_id = $1`, record.ID).Scan(&status); err != nil {
		t.Fatalf("query migration status: %v", err)
	}
	if status != migration.StatusSucceeded {
		t.Fatalf("migration status = %q, want %q", status, migration.StatusSucceeded)
	}
	var progress migration.Progress
	var validation migration.Validation
	if err := pool.QueryRow(ctx, `
SELECT progress, validation_result
FROM platform.data_migration
WHERE migration_id = $1`, record.ID).Scan(&progress, &validation); err != nil {
		t.Fatalf("query migration report: %v", err)
	}
	if progress != (migration.Progress{SessionCount: 1, SessionsCopied: 1, SessionsChecked: 1}) {
		t.Fatalf("migration progress = %+v", progress)
	}
	if validation != (migration.Validation{SessionsVerified: 1}) {
		t.Fatalf("migration validation = %+v", validation)
	}

	targetExec := dataMigrationExecution(t, key, v2)
	target, err := runtime.postgresSessions.ResolveSession(ctx, targetExec)
	if err != nil {
		t.Fatalf("resolve postgres target session: %v", err)
	}
	targetSession, err := target.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("read migrated session: %v", err)
	}
	if targetSession == nil || string(targetSession.State["topic"]) != "billing" || len(targetSession.Events) != 2 {
		t.Fatalf("migrated session = %#v", targetSession)
	}
}

func TestWorkerRuntimeFailsMigrationWhenSourceProviderUnavailable(t *testing.T) {
	if *dataMigrationTestDSN == "" || *dataMigrationTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN and TRPC_AGENT_SERVICE_REDIS_TEST_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-copy-failure-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("copyfail%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Migration Copy Failure", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration Copy Failure", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}

	key := dataMigrationSessionKey(t, tenantID, appID)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("insert session lane: %v", err)
	}
	source, err := redisprovider.NewService(redisprovider.WithRedisClientURL(*dataMigrationTestURL))
	if err != nil {
		t.Fatalf("create redis session service: %v", err)
	}
	t.Cleanup(func() {
		_ = source.DeleteSession(context.Background(), key)
		_ = source.Close()
	})
	if _, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")}); err != nil {
		t.Fatalf("create source session: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-copy-failure-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	if _, err := store.BeginDataMigration(ctx, tenantID, appID, record.ID, "worker-copy-failure", time.Now().Add(time.Minute), dataMigrationLease); err != nil {
		t.Fatalf("begin data migration: %v", err)
	}

	runtime := newDataMigrationTestRuntime(t, ctx, store, seed, "worker-copy-failure", "redis://127.0.0.1:1/0")
	if err := runtime.runDataMigrationPass(ctx); err != nil {
		t.Fatalf("run data migration pass: %v", err)
	}
	assertDataMigrationFailed(t, ctx, pool, store, tenantID, appID, record.ID, v1.Version)
}

func TestWorkerRuntimeFailsMigrationWhenDrainDeadlineExpires(t *testing.T) {
	if *dataMigrationTestDSN == "" || *dataMigrationTestURL == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN and TRPC_AGENT_SERVICE_REDIS_TEST_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-drain-deadline-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("drain%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Migration Drain Deadline", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration Drain Deadline", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}
	key := dataMigrationSessionKey(t, tenantID, appID)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("insert session lane: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.execution (
    tenant_id, app_id, request_id, session_principal_id, session_id, user_id,
    turn_seq, config_version, tenant_source, source_id, idempotency_key,
    payload_hash, command, status, trace_id
) VALUES (
    $1, $2, 'drain-deadline', $3, $4, $3,
    1, $5, 'authenticated_claims', 'drain-deadline', 'drain-deadline',
    decode(repeat('00', 32), 'hex'), '{}'::jsonb, 'PENDING', 'drain-deadline'
)`, tenantID, appID, key.UserID, key.SessionID, v1.Version); err != nil {
		t.Fatalf("insert draining execution: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-drain-deadline-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	if _, err := store.BeginDataMigration(ctx, tenantID, appID, record.ID, "worker-drain-deadline", time.Now().Add(time.Minute), dataMigrationLease); err != nil {
		t.Fatalf("begin data migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.data_migration SET drain_deadline = clock_timestamp() - interval '1 second' WHERE migration_id = $1`, record.ID); err != nil {
		t.Fatalf("expire migration drain deadline: %v", err)
	}

	runtime := newDataMigrationTestRuntime(t, ctx, store, seed, "worker-drain-deadline", *dataMigrationTestURL)
	if err := runtime.runDataMigrationPass(ctx); err != nil {
		t.Fatalf("run data migration pass: %v", err)
	}
	assertDataMigrationFailed(t, ctx, pool, store, tenantID, appID, record.ID, v1.Version)
}

func TestDataMigrationLeaseTakeoverRejectsPreviousWorker(t *testing.T) {
	if *dataMigrationTestDSN == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-takeover-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("takeover%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Migration Takeover", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration Takeover", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-takeover-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	previous, err := store.BeginDataMigration(ctx, tenantID, appID, record.ID, "worker-previous", time.Now().Add(time.Minute), dataMigrationLease)
	if err != nil {
		t.Fatalf("begin data migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.data_migration SET lease_until = clock_timestamp() - interval '1 second' WHERE migration_id = $1`, record.ID); err != nil {
		t.Fatalf("expire migration lease: %v", err)
	}
	claimed, found, err := store.ClaimNextDataMigration(ctx, "worker-successor", dataMigrationLease)
	if err != nil {
		t.Fatalf("claim expired data migration: %v", err)
	}
	if !found || claimed.LeaseOwner != "worker-successor" || claimed.RunToken == previous.RunToken {
		t.Fatalf("claimed migration = %+v, previous token = %q", claimed, previous.RunToken)
	}
	if err := store.UpdateDataMigrationReport(ctx, previous, migration.Progress{}, migration.Validation{}, ""); !errors.Is(err, migration.ErrLeaseLost) {
		t.Fatalf("previous worker update error = %v, want lease lost", err)
	}
}

func TestArtifactCleanupLeaseTakeoverRejectsPreviousWorker(t *testing.T) {
	if *dataMigrationTestDSN == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("artifact-cleanup-takeover-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("cleanup%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, _ := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Artifact Cleanup Takeover", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Artifact Cleanup Takeover", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	key := dataMigrationSessionKey(t, tenantID, appID)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("insert session lane: %v", err)
	}
	record := platformartifact.CleanupRecord{
		ID:                 fmt.Sprintf("artifact-cleanup-%x", seed),
		TenantID:           tenantID,
		AppID:              appID,
		ConfigVersion:      v1.Version,
		SessionPrincipalID: key.UserID,
		SessionID:          key.SessionID,
		Filename:           "report.txt",
		ObjectKey:          "artifact-key",
		Version:            0,
		Status:             platformartifact.CleanupPending,
		LastError:          "delete failed",
	}
	if err := store.EnqueueArtifactCleanup(ctx, record); err != nil {
		t.Fatalf("enqueue artifact cleanup: %v", err)
	}
	previous, found, err := store.ClaimNextArtifactCleanup(ctx, "worker-previous", artifactCleanupLease)
	if err != nil || !found {
		t.Fatalf("claim artifact cleanup found=%v err=%v", found, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.artifact_cleanup SET lease_until = clock_timestamp() - interval '1 second' WHERE cleanup_id = $1`, record.ID); err != nil {
		t.Fatalf("expire cleanup lease: %v", err)
	}
	successor, found, err := store.ClaimNextArtifactCleanup(ctx, "worker-successor", artifactCleanupLease)
	if err != nil || !found || successor.RunToken == previous.RunToken {
		t.Fatalf("claim successor cleanup found=%v record=%+v err=%v", found, successor, err)
	}
	if err := store.CompleteArtifactCleanup(ctx, previous); !errors.Is(err, platformartifact.ErrCleanupLeaseLost) {
		t.Fatalf("previous worker complete error = %v, want lease lost", err)
	}
	if err := store.CompleteArtifactCleanup(ctx, successor); err != nil {
		t.Fatalf("complete successor cleanup: %v", err)
	}
	var status platformartifact.CleanupStatus
	if err := pool.QueryRow(ctx, `SELECT status FROM platform.artifact_cleanup WHERE cleanup_id = $1`, record.ID).Scan(&status); err != nil {
		t.Fatalf("query cleanup status: %v", err)
	}
	if status != platformartifact.CleanupSucceeded {
		t.Fatalf("cleanup status = %q, want %q", status, platformartifact.CleanupSucceeded)
	}
}

func TestWorkerRuntimeCompletesAndRetriesArtifactCleanup(t *testing.T) {
	if *dataMigrationTestDSN == "" {
		t.Skip("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("artifact-cleanup-worker-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("cleanupworker%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, _ := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Artifact Cleanup Worker", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Artifact Cleanup Worker", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	key := dataMigrationSessionKey(t, tenantID, appID)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("insert session lane: %v", err)
	}
	record := platformartifact.CleanupRecord{
		ID:                 fmt.Sprintf("artifact-cleanup-worker-%x", seed),
		TenantID:           tenantID,
		AppID:              appID,
		ConfigVersion:      v1.Version,
		SessionPrincipalID: key.UserID,
		SessionID:          key.SessionID,
		Filename:           "report.txt",
		ObjectKey:          "artifact-key",
		Version:            0,
		Status:             platformartifact.CleanupPending,
		LastError:          "delete failed",
	}
	if err := store.EnqueueArtifactCleanup(ctx, record); err != nil {
		t.Fatalf("enqueue artifact cleanup: %v", err)
	}
	cleanup := &testArtifactCleanupExecutor{}
	runtime := &workerRuntime{store: store, artifactServices: cleanup, owner: "worker-cleanup"}
	if err := runtime.runArtifactCleanupPass(ctx); err != nil {
		t.Fatalf("run artifact cleanup pass: %v", err)
	}
	if cleanup.calls != 1 || cleanup.filename != record.Filename || cleanup.configVersion != v1.Version {
		t.Fatalf("cleanup invocation = %#v", cleanup)
	}
	var status platformartifact.CleanupStatus
	if err := pool.QueryRow(ctx, `SELECT status FROM platform.artifact_cleanup WHERE cleanup_id = $1`, record.ID).Scan(&status); err != nil {
		t.Fatalf("query cleanup status: %v", err)
	}
	if status != platformartifact.CleanupSucceeded {
		t.Fatalf("cleanup status = %q, want %q", status, platformartifact.CleanupSucceeded)
	}

	retryRecord := record
	retryRecord.ID += "-retry"
	retryRecord.Filename = "retry.txt"
	if err := store.EnqueueArtifactCleanup(ctx, retryRecord); err != nil {
		t.Fatalf("enqueue retry artifact cleanup: %v", err)
	}
	retryErr := errors.New("cos delete unavailable")
	cleanup.err = retryErr
	if err := runtime.runArtifactCleanupPass(ctx); err != nil {
		t.Fatalf("run retry artifact cleanup pass: %v", err)
	}
	var lastError string
	if err := pool.QueryRow(ctx, `SELECT status, last_error FROM platform.artifact_cleanup WHERE cleanup_id = $1`, retryRecord.ID).Scan(&status, &lastError); err != nil {
		t.Fatalf("query retry cleanup status: %v", err)
	}
	if status != platformartifact.CleanupPending || !strings.Contains(lastError, retryErr.Error()) {
		t.Fatalf("retry cleanup status=%q last_error=%q", status, lastError)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.artifact_cleanup SET next_attempt_at = clock_timestamp() WHERE cleanup_id = $1`, retryRecord.ID); err != nil {
		t.Fatalf("make retry cleanup due: %v", err)
	}
	cleanup.err = nil
	if err := runtime.runArtifactCleanupPass(ctx); err != nil {
		t.Fatalf("run successful retry cleanup pass: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM platform.artifact_cleanup WHERE cleanup_id = $1`, retryRecord.ID).Scan(&status); err != nil {
		t.Fatalf("query completed retry cleanup status: %v", err)
	}
	if status != platformartifact.CleanupSucceeded {
		t.Fatalf("completed retry cleanup status = %q, want %q", status, platformartifact.CleanupSucceeded)
	}
}

func appendMigrationTestEvents(ctx context.Context, service session.Service, value *session.Session) error {
	if err := service.AppendEvent(ctx, value, event.NewResponseEvent("event-1", "user", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewUserMessage("summarize billing"),
		}},
	})); err != nil {
		return err
	}
	return service.AppendEvent(ctx, value, event.NewResponseEvent("event-2", "assistant", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewAssistantMessage("billing reply"),
		}},
	}))
}

func dataMigrationAppConfigs(tenantID, appID, schema string) (tenant.AppConfig, tenant.AppConfig) {
	base := tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Model: tenant.ModelConfig{
			Provider:  "openai",
			Model:     "gpt-4.1-mini",
			APIKeyRef: tenant.SecretRef{Name: "model-key", Version: "1"},
		},
		SecretRefs: []tenant.SecretRef{{Name: "model-key", Version: "1"}},
	}
	v1 := base
	v1.Version = "v1"
	v1.BackendConfig = tenant.BackendConfig{
		Name: "redis-source",
		Session: tenant.BackendRef{
			Kind:     tenant.BackendRedis,
			Provider: "redis",
			Name:     "redis-source",
		},
	}
	v2 := base
	v2.Version = "v2"
	v2.BackendConfig = tenant.BackendConfig{
		Name: "postgres-target",
		Session: tenant.BackendRef{
			Kind:     tenant.BackendSQL,
			Provider: "postgres",
			Name:     "postgres-target",
			Options:  map[string]string{"schema": schema},
		},
	}
	return v1, v2
}

func dataMigrationExecution(t *testing.T, key session.Key, config tenant.AppConfig) worker.Execution {
	t.Helper()
	runtime := tenant.RuntimeContext{
		TenantID:           config.TenantID,
		AppID:              config.AppID,
		ConfigVersion:      config.Version,
		SessionPrincipalID: key.UserID,
		SessionID:          key.SessionID,
		UserID:             key.UserID,
		TraceID:            "migration-integration",
	}
	handles, err := (storage.StaticResolver{}).Resolve(context.Background(), runtime, config.BackendConfig)
	if err != nil {
		t.Fatalf("resolve storage handles: %v", err)
	}
	return worker.Execution{Tenant: runtime, Config: config, Storage: handles}
}

type testArtifactCleanupExecutor struct {
	calls         int
	filename      string
	configVersion string
	err           error
}

func (e *testArtifactCleanupExecutor) DeleteCleanup(_ context.Context, exec worker.Execution, record platformartifact.CleanupRecord) error {
	e.calls++
	e.filename = record.Filename
	e.configVersion = exec.Tenant.ConfigVersion
	return e.err
}

func dataMigrationSessionKey(t *testing.T, tenantID, appID string) session.Key {
	t.Helper()
	appName, err := (tenant.Scope{TenantID: tenantID, AppID: appID}).Key("runner")
	if err != nil {
		t.Fatalf("build session app name: %v", err)
	}
	return session.Key{AppName: appName, UserID: "user-1", SessionID: "session-1"}
}

func newDataMigrationTestRuntime(
	t *testing.T,
	ctx context.Context,
	store *postgres.Store,
	seed int64,
	owner string,
	sessionRedisURL string,
) *workerRuntime {
	t.Helper()
	redisClient, err := platformredis.NewClient(ctx, *dataMigrationTestURL)
	if err != nil {
		t.Fatalf("create platform redis client: %v", err)
	}
	stream, err := platformredis.NewStream(redisClient, fmt.Sprintf("migration-test:%x", seed), "integration", time.Second)
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("create redis stream: %v", err)
	}
	runtime, err := newWorkerRuntime(store, *dataMigrationTestDSN, sessionRedisURL, "", redisClient, stream, owner, environmentReader(nil))
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("new worker runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.close() })
	return runtime
}

func assertDataMigrationFailed(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	store *postgres.Store,
	tenantID, appID, migrationID, sourceVersion string,
) {
	t.Helper()
	var status migration.Status
	var reason string
	if err := pool.QueryRow(ctx, `
SELECT status, failure_reason
FROM platform.data_migration
WHERE migration_id = $1`, migrationID).Scan(&status, &reason); err != nil {
		t.Fatalf("query failed migration: %v", err)
	}
	if status != migration.StatusFailed || reason == "" {
		t.Fatalf("failed migration status=%q reason=%q", status, reason)
	}
	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after failed migration: %v", err)
	}
	if app.ActiveConfigVersion != sourceVersion {
		t.Fatalf("active config after failed migration = %q, want %q", app.ActiveConfigVersion, sourceVersion)
	}
}

func openDataMigrationIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(*dataMigrationTestDSN)
	if err != nil {
		t.Fatalf("parse postgres test DSN: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open postgres integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func cleanupDataMigrationIntegration(t *testing.T, pool *pgxpool.Pool, tenantID, appID, schema string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `DELETE FROM platform.artifact_cleanup WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete artifact cleanup: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM platform.execution WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete execution: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM platform.data_migration WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete data migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM platform.session_lane WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete session lane: %v", err)
	}
	// Published AppConfig rows are intentionally immutable. The test uses a
	// unique tenant ID and the dedicated _test database, so control-plane rows
	// remain as audit history while mutable migration data is removed above.
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Errorf("drop target session schema: %v", err)
	}
}
