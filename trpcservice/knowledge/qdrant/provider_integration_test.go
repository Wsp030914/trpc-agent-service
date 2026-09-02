//go:build integration

package qdrant_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

const (
	postgresTestDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"
	qdrantTestHostEnv  = "TRPC_AGENT_SERVICE_QDRANT_TEST_HOST"
	qdrantTestPortEnv  = "TRPC_AGENT_SERVICE_QDRANT_TEST_PORT"
)

func TestScopedKnowledgeUsesQdrantFilterAndSQLAuthority(t *testing.T) {
	host := os.Getenv(qdrantTestHostEnv)
	if host == "" {
		t.Skipf("%s is not set", qdrantTestHostEnv)
	}
	port := 6334
	if value := os.Getenv(qdrantTestPortEnv); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			t.Fatalf("parse qdrant port: %v", err)
		}
		port = parsed
	}
	pool := openKnowledgeIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	metadata, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := metadata.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	id := fmt.Sprintf("qdrant-%d", time.Now().UnixNano())
	scope := tenant.Scope{TenantID: id, AppID: "knowledge"}
	createKnowledgeTenant(t, ctx, metadata, scope, id)
	v1 := knowledgeConfig(scope, "v1")
	if err := metadata.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            scope.TenantID,
		AppID:               scope.AppID,
		Name:                "Knowledge",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create app: %v", err)
	}
	base := platformknowledge.Base{Scope: scope, ID: "handbook", Status: platformknowledge.BaseStatusActive}
	if err := metadata.CreateKnowledgeBase(ctx, base); err != nil {
		t.Fatalf("create base: %v", err)
	}
	v2 := v1
	v2.Version = "v2"
	v2.KnowledgeBaseIDs = []string{base.ID}
	if err := metadata.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert bound config: %v", err)
	}

	source := platformknowledge.Document{
		Scope:           scope,
		KnowledgeBaseID: base.ID,
		ID:              "employee-handbook",
		Version:         1,
		ObjectKey:       "knowledge/source.txt",
		ContentSHA256:   bytes.Repeat([]byte{1}, 32),
		MIMEType:        "text/plain",
		Status:          platformknowledge.DocumentStatusAvailable,
		IndexGeneration: "g1",
	}
	if err := metadata.CreateKnowledgeDocument(ctx, source); err != nil {
		t.Fatalf("create document: %v", err)
	}
	chunk := platformknowledge.Chunk{Document: source, ChunkID: "1", Status: platformknowledge.ChunkStatusAvailable}
	if err := metadata.CreateKnowledgeChunk(ctx, chunk); err != nil {
		t.Fatalf("create chunk: %v", err)
	}

	vectorStore, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithHost(host),
		frameworkqdrant.WithPort(port),
		frameworkqdrant.WithCollectionName("knowledge-integration-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
		frameworkqdrant.WithDimension(3),
	)
	if err != nil {
		t.Fatalf("new qdrant store: %v", err)
	}
	defer vectorStore.Close()
	allowed := knowledgeVectorDocument(scope, base.ID, source.ID, "1", "1", "g1", "allowed")
	foreign := knowledgeVectorDocument(tenant.Scope{TenantID: "other", AppID: scope.AppID}, base.ID, source.ID, "1", "foreign", "g1", "foreign")
	if err := vectorStore.Add(ctx, allowed, []float64{1, 0, 0}); err != nil {
		t.Fatalf("add allowed vector: %v", err)
	}
	if err := vectorStore.Add(ctx, foreign, []float64{1, 0, 0}); err != nil {
		t.Fatalf("add foreign vector: %v", err)
	}
	inner := frameworkknowledge.New(
		frameworkknowledge.WithVectorStore(vectorStore),
		frameworkknowledge.WithEmbedder(fixedEmbedder{}),
	)
	service, err := platformknowledge.NewScopedKnowledge(inner, metadata, scope, v2.Version, []string{base.ID})
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := service.Search(ctx, &frameworkknowledge.SearchRequest{Query: "policy", MaxResults: 10})
	if err != nil {
		t.Fatalf("search knowledge: %v", err)
	}
	if len(result.Documents) != 1 || result.Documents[0].Document.ID != allowed.ID {
		t.Fatalf("scoped result = %#v, want only %q", result.Documents, allowed.ID)
	}

	if _, err := pool.Exec(ctx, `
UPDATE platform.knowledge_base
SET status = 'DELETED', updated_at = now()
WHERE tenant_id = $1 AND app_id = $2 AND knowledge_base_id = $3`, scope.TenantID, scope.AppID, base.ID); err != nil {
		t.Fatalf("delete base: %v", err)
	}
	result, err = service.Search(ctx, &frameworkknowledge.SearchRequest{Query: "policy", MaxResults: 10})
	if err != nil {
		t.Fatalf("search deleted base: %v", err)
	}
	if len(result.Documents) != 0 {
		t.Fatalf("deleted base result = %#v, want no documents", result.Documents)
	}
}

type fixedEmbedder struct{}

func (fixedEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return []float64{1, 0, 0}, nil
}

func (fixedEmbedder) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	return []float64{1, 0, 0}, nil, nil
}

func (fixedEmbedder) GetDimensions() int { return 3 }

func openKnowledgeIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(postgresTestDSNEnv)
	if dsn == "" {
		t.Skipf("%s is not set", postgresTestDSNEnv)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres dsn: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open postgres pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createKnowledgeTenant(t *testing.T, ctx context.Context, store *platformpostgres.Store, scope tenant.Scope, name string) {
	t.Helper()
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     scope.TenantID,
		Name:   name,
		Status: tenant.StatusActive,
		Audit: tenant.AuditPolicy{
			Enabled:       true,
			RetentionDays: 30,
			RedactPII:     true,
		},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
}

func knowledgeConfig(scope tenant.Scope, version string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: scope.TenantID,
		AppID:    scope.AppID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:  "openai",
			Model:     "gpt-4.1-mini",
			APIKeyRef: tenant.SecretRef{Name: "model-key", Version: "1"},
		},
		Tools: tenant.ToolPolicy{},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-postgres",
				Options:  map[string]string{"schema": "agent"},
			},
		},
		Audit:      tenant.AuditPolicy{Enabled: true, RetentionDays: 30, RedactPII: true},
		SecretRefs: []tenant.SecretRef{{Name: "model-key", Version: "1"}},
	}
}

func knowledgeVectorDocument(scope tenant.Scope, baseID, documentID, documentVersion, chunkID, generation, id string) *document.Document {
	return &document.Document{
		ID:      id,
		Content: "content " + id,
		Metadata: map[string]any{
			platformknowledge.MetadataTenantID:        scope.TenantID,
			platformknowledge.MetadataAppID:           scope.AppID,
			platformknowledge.MetadataKnowledgeBaseID: baseID,
			platformknowledge.MetadataDocumentID:      documentID,
			platformknowledge.MetadataDocumentVersion: documentVersion,
			platformknowledge.MetadataChunkID:         chunkID,
			platformknowledge.MetadataIndexGeneration: generation,
		},
	}
}
