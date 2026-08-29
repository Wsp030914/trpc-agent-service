//go:build integration

package main

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	knowledgecos "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/importer"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
)

const acceptancePostgresDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_ACCEPTANCE_DSN"

func TestExternalKnowledgePipeline(t *testing.T) {
	dsn := os.Getenv(acceptancePostgresDSNEnv)
	if dsn == "" {
		t.Skipf("%s is not set", acceptancePostgresDSNEnv)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open acceptance database: %v", err)
	}
	t.Cleanup(pool.Close)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new metadata store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate acceptance database: %v", err)
	}

	scope := tenant.Scope{TenantID: "tenant-xxn", AppID: "knowledge"}
	v1, v2 := acceptanceConfigs(scope)
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     scope.TenantID,
		Name:   "External knowledge acceptance",
		Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create acceptance tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            scope.TenantID,
		AppID:               scope.AppID,
		Name:                "Knowledge acceptance",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create acceptance app: %v", err)
	}
	if err := store.CreateKnowledgeBase(ctx, acceptanceKnowledgeBase(scope)); err != nil {
		t.Fatalf("create acceptance knowledge base: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("publish acceptance config: %v", err)
	}
	if err := store.ActivateAppConfig(ctx, scope.TenantID, scope.AppID, v2.Version); err != nil {
		t.Fatalf("activate acceptance config: %v", err)
	}

	exec := acceptanceExecution(t, scope, v2)
	secrets := environmentSecretProvider{getenv: os.Getenv}
	sources, err := knowledgecos.NewResolver(secrets, environmentCOSEndpointResolver{getenv: os.Getenv})
	if err != nil {
		t.Fatalf("new COS source resolver: %v", err)
	}
	t.Cleanup(func() { _ = sources.Close() })
	vectors, err := knowledgeqdrant.NewResolver(
		secrets,
		environmentQdrantEndpointResolver{getenv: os.Getenv},
		store,
		defaultEndpointPolicy{},
	)
	if err != nil {
		t.Fatalf("new Qdrant resolver: %v", err)
	}
	t.Cleanup(func() { _ = vectors.Close() })
	flow, err := importer.New(sources, store)
	if err != nil {
		t.Fatalf("new knowledge importer: %v", err)
	}
	content := []byte("The external acceptance handbook states that retrieval is scoped by tenant and application.")
	document, err := flow.Import(ctx, exec, importer.Input{
		KnowledgeBaseID: "acceptance-handbook",
		DocumentID:      "scope-policy",
		Version:         1,
		Content:         content,
		MIMEType:        "text/plain",
	})
	if err != nil {
		t.Fatalf("import COS source: %v", err)
	}
	stored, err := sources.GetSource(ctx, exec, document)
	if err != nil {
		t.Fatalf("read COS source: %v", err)
	}
	if !bytes.Equal(stored, content) {
		t.Fatal("COS source content does not match the imported content")
	}

	job, found, err := store.ClaimNextKnowledgeIndex(ctx, "external-acceptance", time.Minute)
	if err != nil {
		t.Fatalf("claim knowledge index job: %v", err)
	}
	if !found {
		t.Fatal("knowledge import did not enqueue an index job")
	}
	runtime := &workerRuntime{
		store:            store,
		knowledgeSources: sources,
		knowledge:        vectors,
		owner:            "external-acceptance",
	}
	if err := runtime.runKnowledgeIndex(ctx, job); err != nil {
		t.Fatalf("index with DashScope and Qdrant: %v", err)
	}
	service, err := vectors.ResolveKnowledge(ctx, exec)
	if err != nil {
		t.Fatalf("resolve scoped knowledge: %v", err)
	}
	result, err := service.Search(ctx, &frameworkknowledge.SearchRequest{
		Query:      "how is retrieval scoped",
		MaxResults: 3,
	})
	if err != nil {
		t.Fatalf("search Qdrant through SQL authority: %v", err)
	}
	if len(result.Documents) == 0 || result.Documents[0].Document == nil || result.Documents[0].Document.ID == "" {
		t.Fatal("scoped knowledge search returned no indexed document")
	}
}

func acceptanceConfigs(scope tenant.Scope) (tenant.AppConfig, tenant.AppConfig) {
	artifact := tenant.BackendRef{
		Kind:      tenant.BackendObject,
		Provider:  "cos",
		Name:      "cos-main",
		SecretRef: tenant.SecretRef{Name: "cos-credentials", Version: "v1"},
	}
	base := tenant.AppConfig{
		TenantID: scope.TenantID,
		AppID:    scope.AppID,
		Model: tenant.ModelConfig{
			Provider:  "openai",
			Model:     "qwen3.7-text-embedding",
			APIKeyRef: tenant.SecretRef{Name: "model-api-key", Version: "v1"},
			Parameters: map[string]string{
				"base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
			},
		},
		BackendConfig: tenant.BackendConfig{
			Name: "external-acceptance",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "postgres",
				Options:  map[string]string{"schema": "agent"},
			},
			Artifact: artifact,
		},
		SecretRefs: []tenant.SecretRef{
			{Name: "cos-credentials", Version: "v1"},
			{Name: "qdrant-api-key", Version: "v1"},
			{Name: "model-api-key", Version: "v1"},
		},
	}
	v1 := base.Clone()
	v1.Version = "external-v1"
	v2 := base.Clone()
	v2.Version = "external-v2"
	v2.KnowledgeBaseIDs = []string{"acceptance-handbook"}
	v2.BackendConfig.Knowledge = tenant.BackendRef{
		Kind:      tenant.BackendVector,
		Provider:  "qdrant",
		Name:      "qdrant-cloud",
		SecretRef: tenant.SecretRef{Name: "qdrant-api-key", Version: "v1"},
		Options: map[string]string{
			"embedding_model":      "qwen3.7-text-embedding",
			"embedding_dimensions": "1024",
			"embedding_profile":    "qwen3-7-text-embedding",
			"index_generation":     "external-g1",
		},
	}
	return v1, v2
}

func acceptanceKnowledgeBase(scope tenant.Scope) platformknowledge.Base {
	return platformknowledge.Base{
		Scope:  scope,
		ID:     "acceptance-handbook",
		Status: platformknowledge.BaseStatusActive,
	}
}

func acceptanceExecution(t *testing.T, scope tenant.Scope, config tenant.AppConfig) worker.Execution {
	t.Helper()
	runtime := tenant.RuntimeContext{
		TenantID:           scope.TenantID,
		AppID:              scope.AppID,
		ConfigVersion:      config.Version,
		SessionPrincipalID: "external-acceptance",
		SessionID:          "external-acceptance",
		UserID:             "external-acceptance",
		TraceID:            "external-acceptance",
	}
	handles, err := (storage.StaticResolver{}).Resolve(context.Background(), runtime, config.BackendConfig)
	if err != nil {
		t.Fatalf("resolve acceptance storage: %v", err)
	}
	return worker.Execution{Tenant: runtime, Config: config, Storage: handles}
}
