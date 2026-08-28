package postgres_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const postgresTestDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"

var postgresTestDSN = flag.String(
	"postgres-test-dsn",
	os.Getenv(postgresTestDSNEnv),
	"dedicated PostgreSQL integration database DSN",
)

func TestPostgresMigrationAndRepositories(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS platform CASCADE; DROP SCHEMA IF EXISTS agent CASCADE`); err != nil {
		t.Fatalf("reset platform and session schemas: %v", err)
	}
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- store.Migrate(ctx)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
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
	} {
		var exists bool
		if err := pool.QueryRow(
			ctx,
			`SELECT to_regclass($1) IS NOT NULL`,
			"platform."+table,
		).Scan(&exists); err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("table platform.%s does not exist", table)
		}
	}

	tenantValue := tenant.Tenant{
		ID:     "tenant-a",
		Name:   "Tenant A",
		Status: tenant.StatusActive,
		Audit: tenant.AuditPolicy{
			Enabled:       true,
			RetentionDays: 30,
			RedactPII:     true,
		},
	}
	if err := store.CreateTenant(ctx, tenantValue); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	resolvedTenant, err := store.ResolveTenant(ctx, tenantValue.ID)
	if err != nil {
		t.Fatalf("resolve tenant: %v", err)
	}
	if !reflect.DeepEqual(resolvedTenant, tenantValue) {
		t.Fatalf("resolved tenant = %#v, want %#v", resolvedTenant, tenantValue)
	}

	v1 := integrationAppConfig("v1", "gpt-4.1-mini")
	app := tenant.AgentApp{
		TenantID:            tenantValue.ID,
		AppID:               v1.AppID,
		Name:                "Support",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}
	if err := store.CreateAgentApp(ctx, app, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	binding := integrationBinding()
	if err := store.CreateChannelBinding(ctx, binding); err != nil {
		t.Fatalf("create channel binding: %v", err)
	}
	resolvedBinding, err := store.ResolveBinding(ctx, binding.TenantID, binding.AppID, binding.BindingID)
	if err != nil {
		t.Fatalf("resolve channel binding: %v", err)
	}
	if !reflect.DeepEqual(resolvedBinding, binding) {
		t.Fatalf("resolved channel binding = %#v, want %#v", resolvedBinding, binding)
	}
	resolvedV1, err := store.ResolveAppConfig(ctx, v1.TenantID, v1.AppID, v1.Version)
	if err != nil {
		t.Fatalf("resolve initial config: %v", err)
	}
	if !reflect.DeepEqual(resolvedV1, v1) {
		t.Fatalf("resolved initial config = %#v, want %#v", resolvedV1, v1)
	}

	tenantB := tenantValue
	tenantB.ID = "tenant-b"
	tenantB.Name = "Tenant B"
	if err := store.CreateTenant(ctx, tenantB); err != nil {
		t.Fatalf("create second tenant: %v", err)
	}
	v1TenantB := integrationAppConfig("v1", "tenant-b-model")
	v1TenantB.TenantID = tenantB.ID
	appTenantB := app
	appTenantB.TenantID = tenantB.ID
	if err := store.CreateAgentApp(ctx, appTenantB, v1TenantB); err != nil {
		t.Fatalf("create same app ID in second tenant: %v", err)
	}
	resolvedTenantBConfig, err := store.ResolveAppConfig(
		ctx,
		v1TenantB.TenantID,
		v1TenantB.AppID,
		v1TenantB.Version,
	)
	if err != nil {
		t.Fatalf("resolve second tenant config: %v", err)
	}
	if resolvedTenantBConfig.Model.Model == resolvedV1.Model.Model {
		t.Fatalf(
			"tenant configs were not isolated: both models are %q",
			resolvedTenantBConfig.Model.Model,
		)
	}

	v2 := integrationAppConfig("v2", "gpt-4.1")
	v2.ChannelBinding = []string{binding.BindingID}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert app config v2: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, v2.TenantID, v2.AppID, v2.Version); err != nil {
		t.Fatalf("activate app config v2: %v", err)
	}
	resolvedApp, err := store.ResolveAgentApp(ctx, app.TenantID, app.AppID)
	if err != nil {
		t.Fatalf("resolve agent app: %v", err)
	}
	if resolvedApp.ActiveConfigVersion != v2.Version {
		t.Fatalf(
			"active config version = %q, want %q",
			resolvedApp.ActiveConfigVersion,
			v2.Version,
		)
	}

	if _, err := pool.Exec(
		ctx,
		`UPDATE platform.app_config_version
SET model_config = '{"provider":"other","model":"changed"}'::jsonb
WHERE tenant_id = $1 AND app_id = $2 AND version = $3`,
		v1.TenantID,
		v1.AppID,
		v1.Version,
	); err == nil {
		t.Fatal("published app config update succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
			t.Fatalf("immutable config error = %v, want SQLSTATE 55000", err)
		}
	}

	digest, err := auth.DigestAPIKey("tas_integration_key_with_at_least_32_random_bytes")
	if err != nil {
		t.Fatalf("digest API key: %v", err)
	}
	expiresAt := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
	credential := auth.Credential{
		ID:        "credential-1",
		TenantID:  app.TenantID,
		AppID:     app.AppID,
		KeyPrefix: "tas_inte",
		Status:    auth.CredentialActive,
		ExpiresAt: expiresAt,
	}
	if err := store.CreateCredential(ctx, digest, credential); err != nil {
		t.Fatalf("create credential: %v", err)
	}
	resolvedCredential, err := store.ResolveAPIKey(ctx, digest)
	if err != nil {
		t.Fatalf("resolve credential: %v", err)
	}
	if !reflect.DeepEqual(resolvedCredential, credential) {
		t.Fatalf(
			"resolved credential = %#v, want %#v",
			resolvedCredential,
			credential,
		)
	}
	identity := gateway.AdmissionIdentity{
		Tenant: tenant.RuntimeContext{
			TenantID:           app.TenantID,
			AppID:              app.AppID,
			ConfigVersion:      "stale-v1",
			SessionID:          "session-admission",
			SessionPrincipalID: "principal-admission",
			UserID:             "user-admission",
			TraceID:            "trace-admission-1",
		},
		Source:           gateway.TenantSourceAuthenticatedClaims,
		SourceID:         credential.ID,
		CredentialDigest: gateway.CredentialDigest(digest),
	}
	admissionRequest := gateway.AdmissionRequest{
		RequestID:      "request-admission-1",
		IdempotencyKey: "client-admission-1",
		Identity:       identity,
		Message:        gateway.Message{Text: "first admission"},
	}
	admissionResult, err := store.Admit(ctx, admissionRequest)
	if err != nil {
		t.Fatalf("admit request: %v", err)
	}
	if admissionResult.ConfigVersion != v2.Version || admissionResult.TurnSeq != 1 || admissionResult.Replayed {
		t.Fatalf("admission result = %#v", admissionResult)
	}
	var outboxCount int
	if err := pool.QueryRow(
		ctx,
		`SELECT count(*)
FROM platform.dispatch_outbox
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'PENDING'`,
		app.TenantID,
		app.AppID,
		admissionResult.RequestID,
	).Scan(&outboxCount); err != nil {
		t.Fatalf("query admission dispatch outbox: %v", err)
	}
	if outboxCount != 1 {
		t.Fatalf("admission dispatch outbox count = %d, want 1", outboxCount)
	}
	dispatches, err := store.ClaimDispatches(ctx, "relay-1", time.Second, 1)
	if err != nil || len(dispatches) != 1 {
		t.Fatalf("claim dispatches = %#v, %v", dispatches, err)
	}
	if err := store.CompleteDispatch(ctx, dispatches[0], "relay-1"); err != nil {
		t.Fatalf("complete dispatch: %v", err)
	}
	claim, found, err := store.Claim(ctx, dispatches[0], queue.ClaimRequest{Owner: "worker-1", LeaseDuration: time.Second})
	if err != nil || !found {
		t.Fatalf("claim execution = %#v, %t, %v", claim, found, err)
	}
	if err := store.Complete(ctx, claim, queue.CompletionSucceeded); err != nil {
		t.Fatalf("complete execution: %v", err)
	}

	replayed, err := store.Admit(ctx, admissionRequest)
	if err != nil {
		t.Fatalf("replay admission: %v", err)
	}
	if !replayed.Replayed || replayed.RequestID != admissionResult.RequestID || replayed.TurnSeq != admissionResult.TurnSeq {
		t.Fatalf("replayed admission result = %#v", replayed)
	}

	conflictingRequest := admissionRequest
	conflictingRequest.Message.Text = "different payload"
	if _, err := store.Admit(ctx, conflictingRequest); !errors.Is(err, gateway.ErrIdempotencyConflict) {
		t.Fatalf("conflicting admission error = %v, want idempotency conflict", err)
	}

	differentSessionRequest := admissionRequest
	differentSessionRequest.Identity.Tenant.SessionID = "another-session"
	if _, err := store.Admit(ctx, differentSessionRequest); !errors.Is(err, gateway.ErrIdempotencyConflict) {
		t.Fatalf("different-session admission error = %v, want idempotency conflict", err)
	}

	v3 := integrationAppConfig("v3", "gpt-4.1-nano")
	if err := store.InsertAppConfigVersion(ctx, v3); err != nil {
		t.Fatalf("insert app config v3: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, v3.TenantID, v3.AppID, v3.Version); err != nil {
		t.Fatalf("activate app config v3: %v", err)
	}
	secondRequest := admissionRequest
	secondRequest.RequestID = "request-admission-2"
	secondRequest.IdempotencyKey = "client-admission-2"
	secondRequest.Identity.Tenant.TraceID = "trace-admission-2"
	second, err := store.Admit(ctx, secondRequest)
	if err != nil {
		t.Fatalf("admit request after config switch: %v", err)
	}
	if second.ConfigVersion != v3.Version || second.TurnSeq != 2 {
		t.Fatalf("second admission result = %#v", second)
	}

	var pinnedVersions string
	if err := pool.QueryRow(
		ctx,
		`SELECT string_agg(config_version, ',' ORDER BY turn_seq)
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND session_principal_id = $3 AND session_id = $4`,
		app.TenantID,
		app.AppID,
		identity.Tenant.SessionPrincipalID,
		identity.Tenant.SessionID,
	).Scan(&pinnedVersions); err != nil {
		t.Fatalf("query pinned config versions: %v", err)
	}
	if pinnedVersions != "v2,v3" {
		t.Fatalf("pinned config versions = %q, want v2,v3", pinnedVersions)
	}

	if err := store.RevokeCredential(ctx, credential.TenantID, credential.AppID, credential.ID); err != nil {
		t.Fatalf("revoke credential: %v", err)
	}
	revokedCredential, err := store.ResolveAPIKey(ctx, digest)
	if err != nil {
		t.Fatalf("resolve revoked credential: %v", err)
	}
	if revokedCredential.Status != auth.CredentialRevoked {
		t.Fatalf("revoked credential status = %q", revokedCredential.Status)
	}
	revokedRequest := secondRequest
	revokedRequest.RequestID = "request-admission-revoked"
	revokedRequest.IdempotencyKey = "client-admission-revoked"
	if _, err := store.Admit(ctx, revokedRequest); !errors.Is(err, auth.ErrCredentialInactive) {
		t.Fatalf("admit with revoked credential error = %v, want inactive credential", err)
	}
	if _, err := pool.Exec(
		ctx,
		`UPDATE platform.api_credential
SET status = 'ACTIVE'
WHERE tenant_id = $1 AND app_id = $2 AND credential_id = $3`,
		credential.TenantID,
		credential.AppID,
		credential.ID,
	); err == nil {
		t.Fatal("reactivate revoked credential succeeded")
	} else {
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55000" {
			t.Fatalf("reactivate revoked credential error = %v, want SQLSTATE 55000", err)
		}
	}

	if _, err := store.ResolveTenant(ctx, "missing"); !errors.Is(err, platformpostgres.ErrNotFound) {
		t.Fatalf("resolve missing tenant error = %v, want not found", err)
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
				Kind:    tenant.BackendSQL,
				Name:    "session-postgres",
				Options: map[string]string{"schema": "agent"},
			},
		},
		Audit: tenant.AuditPolicy{
			Enabled:       true,
			RetentionDays: 30,
			RedactPII:     true,
		},
		SecretRefs: []tenant.SecretRef{{Name: "model-key", Version: "1"}},
	}
}

func integrationBinding() channels.Binding {
	return channels.Binding{
		TenantID:         "tenant-a",
		AppID:            "support",
		BindingID:        "wecom-support",
		Channel:          channels.ChannelWeCom,
		ExternalAccount:  "corp-agent-support",
		WebhookURL:       "https://example.com/im/wecom/support",
		TokenRef:         tenant.SecretRef{Name: "wecom-token", Version: "1"},
		SigningSecretRef: tenant.SecretRef{Name: "wecom-signing", Version: "1"},
		Status:           channels.BindingActive,
	}
}
