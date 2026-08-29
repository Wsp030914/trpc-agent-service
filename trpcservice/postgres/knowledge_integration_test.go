package postgres_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestKnowledgeCatalogAuthorizesOnlyBoundAvailableChunks(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	id := fmt.Sprintf("knowledge-%d", time.Now().UnixNano())
	scope := tenant.Scope{TenantID: id, AppID: "support"}
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     scope.TenantID,
		Name:   id,
		Status: tenant.StatusActive,
		Audit: tenant.AuditPolicy{
			Enabled:       true,
			RetentionDays: 30,
			RedactPII:     true,
		},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}

	v1 := integrationAppConfig("v1", "gpt-4.1-mini")
	v1.TenantID = scope.TenantID
	v1.AppID = scope.AppID
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            scope.TenantID,
		AppID:               scope.AppID,
		Name:                "Support",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}

	base := platformknowledge.Base{Scope: scope, ID: "handbook", Status: platformknowledge.BaseStatusActive}
	if err := store.CreateKnowledgeBase(ctx, base); err != nil {
		t.Fatalf("create knowledge base: %v", err)
	}
	v2 := v1
	v2.Version = "v2"
	v2.KnowledgeBaseIDs = []string{base.ID}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert knowledge-bound config: %v", err)
	}

	document := platformknowledge.Document{
		Scope:           scope,
		KnowledgeBaseID: base.ID,
		ID:              "employee-handbook",
		Version:         1,
		ObjectKey:       "knowledge/employee-handbook/v1.txt",
		ContentSHA256:   bytes.Repeat([]byte{1}, 32),
		MIMEType:        "text/plain",
		Status:          platformknowledge.DocumentStatusAvailable,
		IndexGeneration: "g1",
	}
	if err := store.CreateKnowledgeDocument(ctx, document); err != nil {
		t.Fatalf("create knowledge document: %v", err)
	}
	chunk := platformknowledge.Chunk{
		Document: document,
		ChunkID:  "chunk-1",
		Status:   platformknowledge.ChunkStatusPending,
	}
	if err := store.CreateKnowledgeChunk(ctx, chunk); err != nil {
		t.Fatalf("create pending knowledge chunk: %v", err)
	}
	ref := platformknowledge.ChunkRef{
		Scope:           scope,
		ConfigVersion:   v2.Version,
		KnowledgeBaseID: base.ID,
		DocumentID:      document.ID,
		DocumentVersion: "1",
		ChunkID:         chunk.ChunkID,
		IndexGeneration: document.IndexGeneration,
	}
	assertKnowledgeChunkAvailable(t, ctx, store, ref, false)
	if err := store.MarkKnowledgeChunkAvailable(ctx, chunk); err != nil {
		t.Fatalf("mark knowledge chunk available: %v", err)
	}
	assertKnowledgeChunkAvailable(t, ctx, store, ref, true)

	job := platformknowledge.IndexJob{
		ID:            "index-" + id,
		Document:      document,
		ConfigVersion: v2.Version,
		BuildID:       v2.Version,
		Status:        platformknowledge.IndexJobPending,
	}
	if err := store.EnqueueKnowledgeIndex(ctx, job); err != nil {
		t.Fatalf("enqueue knowledge index: %v", err)
	}
	first, found, err := store.ClaimNextKnowledgeIndex(ctx, "worker-1", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim knowledge index = %#v, %t, %v", first, found, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE platform.knowledge_index_job
SET lease_until = clock_timestamp() - interval '1 second'
WHERE job_id = $1`, first.ID); err != nil {
		t.Fatalf("expire knowledge index lease: %v", err)
	}
	second, found, err := store.ClaimNextKnowledgeIndex(ctx, "worker-2", time.Minute)
	if err != nil || !found {
		t.Fatalf("take over knowledge index = %#v, %t, %v", second, found, err)
	}
	if err := store.CompleteKnowledgeIndex(ctx, first); !errors.Is(err, platformknowledge.ErrIndexLeaseLost) {
		t.Fatalf("complete previous knowledge index = %v, want lease lost", err)
	}
	if err := store.CompleteKnowledgeIndex(ctx, second); err != nil {
		t.Fatalf("complete knowledge index: %v", err)
	}

	ref.ConfigVersion = v1.Version
	assertKnowledgeChunkAvailable(t, ctx, store, ref, false)
	ref.ConfigVersion = v2.Version
	if _, err := pool.Exec(ctx, `
UPDATE platform.knowledge_base
SET status = 'DELETED', updated_at = now()
WHERE tenant_id = $1 AND app_id = $2 AND knowledge_base_id = $3`,
		scope.TenantID,
		scope.AppID,
		base.ID,
	); err != nil {
		t.Fatalf("delete knowledge base: %v", err)
	}
	assertKnowledgeChunkAvailable(t, ctx, store, ref, false)
}

func TestCreateKnowledgeDocumentAndEnqueueRetriesSameImmutableSource(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	id := fmt.Sprintf("knowledge-retry-%d", time.Now().UnixNano())
	scope := tenant.Scope{TenantID: id, AppID: "support"}
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     scope.TenantID,
		Name:   id,
		Status: tenant.StatusActive,
		Audit:  tenant.AuditPolicy{Enabled: true, RetentionDays: 30, RedactPII: true},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	config := integrationAppConfig("v1", "gpt-4.1-mini")
	config.TenantID = scope.TenantID
	config.AppID = scope.AppID
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: scope.TenantID, AppID: scope.AppID, Name: "Support",
		ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	base := platformknowledge.Base{Scope: scope, ID: "handbook", Status: platformknowledge.BaseStatusActive}
	if err := store.CreateKnowledgeBase(ctx, base); err != nil {
		t.Fatalf("create knowledge base: %v", err)
	}
	bound := config
	bound.Version = "v2"
	bound.KnowledgeBaseIDs = []string{base.ID}
	if err := store.InsertAppConfigVersion(ctx, bound); err != nil {
		t.Fatalf("insert knowledge-bound config: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, scope.TenantID, scope.AppID, bound.Version); err != nil {
		t.Fatalf("activate knowledge-bound config: %v", err)
	}
	document := platformknowledge.Document{
		Scope: scope, KnowledgeBaseID: base.ID, ID: "employee-handbook", Version: 1,
		ObjectKey: "knowledge/employee-handbook/v1.txt", ContentSHA256: bytes.Repeat([]byte{2}, 32),
		MIMEType: "text/plain", Status: platformknowledge.DocumentStatusAvailable, IndexGeneration: "g1",
	}
	for attempt := 0; attempt < 2; attempt++ {
		job := platformknowledge.IndexJob{
			ID: "index-retry-" + id + "-" + strconv.Itoa(attempt), Document: document,
			ConfigVersion: bound.Version, BuildID: bound.Version, Status: platformknowledge.IndexJobPending,
		}
		if err := store.CreateKnowledgeDocumentAndEnqueue(ctx, document, job); err != nil {
			t.Fatalf("create document and enqueue attempt %d: %v", attempt, err)
		}
	}
	var documentCount, activeJobCount int
	if err := pool.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM platform.knowledge_document
      WHERE tenant_id = $1 AND app_id = $2 AND knowledge_base_id = $3 AND document_id = $4),
    (SELECT count(*) FROM platform.knowledge_index_job
      WHERE tenant_id = $1 AND app_id = $2 AND knowledge_base_id = $3 AND document_id = $4
        AND status IN ('PENDING', 'RUNNING'))`,
		scope.TenantID, scope.AppID, base.ID, document.ID,
	).Scan(&documentCount, &activeJobCount); err != nil {
		t.Fatalf("count retry rows: %v", err)
	}
	if documentCount != 1 || activeJobCount != 1 {
		t.Fatalf("retry rows = documents %d active jobs %d, want 1 and 1", documentCount, activeJobCount)
	}
}

func TestActivateAppConfigWaitsForKnowledgeGeneration(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM platform.knowledge_index_job`); err != nil {
		t.Fatalf("clear prior knowledge index jobs: %v", err)
	}

	id := fmt.Sprintf("knowledge-generation-%d", time.Now().UnixNano())
	scope := tenant.Scope{TenantID: id, AppID: "support"}
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: id, Name: id, Status: tenant.StatusActive, Audit: tenant.AuditPolicy{Enabled: true, RetentionDays: 30, RedactPII: true}}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	v1 := integrationAppConfig("v1", "gpt-4.1-mini")
	v1.TenantID, v1.AppID = scope.TenantID, scope.AppID
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{TenantID: scope.TenantID, AppID: scope.AppID, Name: "Support", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	base := platformknowledge.Base{Scope: scope, ID: "handbook", Status: platformknowledge.BaseStatusActive}
	if err := store.CreateKnowledgeBase(ctx, base); err != nil {
		t.Fatalf("create knowledge base: %v", err)
	}
	v2 := v1
	v2.Version = "v2"
	v2.KnowledgeBaseIDs = []string{base.ID}
	v2.BackendConfig.Knowledge = testKnowledgeBackend("g1")
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert g1 config: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, scope.TenantID, scope.AppID, v2.Version); err != nil {
		t.Fatalf("activate g1 config: %v", err)
	}
	document := platformknowledge.Document{Scope: scope, KnowledgeBaseID: base.ID, ID: "employee-handbook", Version: 1, ObjectKey: "knowledge/employee-handbook/v1.txt", ContentSHA256: bytes.Repeat([]byte{3}, 32), MIMEType: "text/plain", Status: platformknowledge.DocumentStatusAvailable, IndexGeneration: "g1"}
	if err := store.CreateKnowledgeDocument(ctx, document); err != nil {
		t.Fatalf("create knowledge document: %v", err)
	}
	v3 := v2
	v3.Version = "v3"
	v3.BackendConfig.Knowledge = testKnowledgeBackend("g2")
	if err := store.InsertAppConfigVersion(ctx, v3); err != nil {
		t.Fatalf("insert g2 config: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, scope.TenantID, scope.AppID, v3.Version); !errors.Is(err, tenant.ErrKnowledgeGenerationPending) {
		t.Fatalf("activate g2 config = %v, want generation pending", err)
	}
	job, found, err := store.ClaimNextKnowledgeIndex(ctx, "worker-generation", time.Minute)
	if err != nil || !found {
		t.Fatalf("claim generation job = %#v, %t, %v", job, found, err)
	}
	if job.Document.IndexGeneration != "g2" || job.ConfigVersion != v3.Version {
		t.Fatalf("generation job = %#v, want g2 and v3", job)
	}
	if err := store.CompleteKnowledgeIndex(ctx, job); err != nil {
		t.Fatalf("complete generation job: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, scope.TenantID, scope.AppID, v3.Version); err != nil {
		t.Fatalf("activate rebuilt g2 config: %v", err)
	}
	app, err := store.ResolveAgentApp(ctx, scope.TenantID, scope.AppID)
	if err != nil || app.ActiveConfigVersion != v3.Version {
		t.Fatalf("active app = %#v, %v; want v3", app, err)
	}
}

func testKnowledgeBackend(generation string) tenant.BackendRef {
	return tenant.BackendRef{
		Kind: tenant.BackendVector, Provider: "qdrant", Name: "shared-qdrant",
		Options: map[string]string{
			"embedding_model": "text-embedding-3-small", "embedding_dimensions": "1536",
			"embedding_profile": "text-embedding-3-small", "index_generation": generation,
		},
	}
}

func assertKnowledgeChunkAvailable(
	t *testing.T,
	ctx context.Context,
	store *platformpostgres.Store,
	ref platformknowledge.ChunkRef,
	want bool,
) {
	t.Helper()
	got, err := store.AvailableKnowledgeChunk(ctx, ref)
	if err != nil {
		t.Fatalf("available knowledge chunk: %v", err)
	}
	if got != want {
		t.Fatalf("available knowledge chunk = %t, want %t", got, want)
	}
}
