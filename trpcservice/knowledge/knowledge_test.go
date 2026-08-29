package knowledge

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

func TestScopedKnowledgeForcesScopeAndSQLAuthorization(t *testing.T) {
	inner := &recordingKnowledge{results: map[string]*frameworkknowledge.SearchResult{
		"base-a": {
			Documents: []*frameworkknowledge.Result{
				{Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "chunk-a", "g1"), Score: 0.9},
				{Document: chunkDocument("tenant-a", "app-a", "base-a", "doc-a", "1", "chunk-denied", "g1"), Score: 0.8},
				{Document: chunkDocument("tenant-b", "app-a", "base-a", "doc-a", "1", "chunk-forged", "g1"), Score: 1},
			},
		},
		"base-b": {
			Documents: []*frameworkknowledge.Result{
				{Document: chunkDocument("tenant-a", "app-a", "base-b", "doc-b", "2", "chunk-b", "g1"), Score: 0.7},
			},
		},
	}}
	catalog := catalogFunc(func(_ context.Context, ref ChunkRef) (bool, error) {
		return ref.ChunkID != "chunk-denied", nil
	})
	view, err := NewScopedKnowledge(inner, catalog, tenant.Scope{TenantID: "tenant-a", AppID: "app-a"}, "v1", []string{"base-b", "base-a"})
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := view.Search(context.Background(), &frameworkknowledge.SearchRequest{
		Query:      "question",
		MaxResults: 1,
		SearchFilter: &frameworkknowledge.SearchFilter{Metadata: map[string]any{
			MetadataTenantID: "forged",
			"category":       "guide",
		}},
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(result.Documents) != 1 || result.Documents[0].Document.ID != "chunk-a" {
		t.Fatalf("authorized documents = %#v", result.Documents)
	}
	if result.Text != "content chunk-a" {
		t.Fatalf("result text = %q", result.Text)
	}
	if len(inner.requests) != 2 {
		t.Fatalf("search calls = %d, want 2", len(inner.requests))
	}
	for baseID, request := range inner.requests {
		if got := request.SearchFilter.Metadata[MetadataTenantID]; got != "tenant-a" {
			t.Fatalf("%s tenant filter = %v", baseID, got)
		}
		if got := request.SearchFilter.Metadata[MetadataAppID]; got != "app-a" {
			t.Fatalf("%s app filter = %v", baseID, got)
		}
		if got := request.SearchFilter.Metadata[MetadataKnowledgeBaseID]; got != baseID {
			t.Fatalf("%s base filter = %v", baseID, got)
		}
		if got := request.SearchFilter.Metadata["category"]; got != "guide" {
			t.Fatalf("%s caller filter = %v", baseID, got)
		}
	}
}

type recordingKnowledge struct {
	results  map[string]*frameworkknowledge.SearchResult
	requests map[string]*frameworkknowledge.SearchRequest
}

func (k *recordingKnowledge) Search(_ context.Context, req *frameworkknowledge.SearchRequest) (*frameworkknowledge.SearchResult, error) {
	if k.requests == nil {
		k.requests = make(map[string]*frameworkknowledge.SearchRequest)
	}
	baseID, _ := req.SearchFilter.Metadata[MetadataKnowledgeBaseID].(string)
	k.requests[baseID] = req
	return k.results[baseID], nil
}

type catalogFunc func(context.Context, ChunkRef) (bool, error)

func (f catalogFunc) AvailableKnowledgeChunk(ctx context.Context, ref ChunkRef) (bool, error) {
	return f(ctx, ref)
}

func chunkDocument(tenantID, appID, baseID, documentID, version, chunkID, generation string) *document.Document {
	return &document.Document{
		ID:      chunkID,
		Content: "content " + chunkID,
		Metadata: map[string]any{
			MetadataTenantID:        tenantID,
			MetadataAppID:           appID,
			MetadataKnowledgeBaseID: baseID,
			MetadataDocumentID:      documentID,
			MetadataDocumentVersion: version,
			MetadataChunkID:         chunkID,
			MetadataIndexGeneration: generation,
		},
	}
}
