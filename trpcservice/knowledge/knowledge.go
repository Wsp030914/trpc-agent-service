// Package knowledge provides tenant-scoped knowledge retrieval contracts.
package knowledge

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/searchfilter"
)

var (
	// ErrIndexLeaseLost reports that another worker owns an index job lease.
	ErrIndexLeaseLost = errors.New("knowledge index job lease lost")
)

const (
	// MetadataTenantID is the required Qdrant payload metadata field.
	MetadataTenantID = "tenant_id"
	// MetadataAppID is the required Qdrant payload metadata field.
	MetadataAppID = "app_id"
	// MetadataKnowledgeBaseID is the required Qdrant payload metadata field.
	MetadataKnowledgeBaseID = "knowledge_base_id"
	// MetadataDocumentID is the required Qdrant payload metadata field.
	MetadataDocumentID = "document_id"
	// MetadataDocumentVersion is the required Qdrant payload metadata field.
	MetadataDocumentVersion = "document_version"
	// MetadataChunkID is the required Qdrant payload metadata field.
	MetadataChunkID = "chunk_id"
	// MetadataIndexGeneration is the required Qdrant payload metadata field.
	MetadataIndexGeneration = "index_generation"

	// scopedSearchInitialOverfetch bounds the first candidate batch while
	// leaving room for SQL authorization to reject vector hits.
	scopedSearchInitialOverfetch = 4
	// scopedSearchMaxOverfetch bounds the extra candidates requested when a
	// provider does not expose a paging cursor through the framework API.
	scopedSearchMaxOverfetch = 256
)

// ChunkRef identifies one derived vector chunk under its authoritative scope.
type ChunkRef struct {
	Scope           tenant.Scope
	ConfigVersion   string
	KnowledgeBaseID string
	DocumentID      string
	DocumentVersion string
	ChunkID         string
	IndexGeneration string
}

// BaseStatus is the authoritative availability state of a knowledge base.
type BaseStatus string

const (
	// BaseStatusActive permits bound configurations to retrieve this base.
	BaseStatusActive BaseStatus = "ACTIVE"
	// BaseStatusDeleted prevents all further retrieval from this base.
	BaseStatusDeleted BaseStatus = "DELETED"
)

// Base identifies one tenant application's knowledge base.
type Base struct {
	Scope  tenant.Scope
	ID     string
	Status BaseStatus
}

// DocumentStatus is the authoritative lifecycle state of a source document.
type DocumentStatus string

const (
	// DocumentStatusPending has recorded metadata but is not readable yet.
	DocumentStatusPending DocumentStatus = "PENDING"
	// DocumentStatusAvailable permits indexing and retrieval of its chunks.
	DocumentStatusAvailable DocumentStatus = "AVAILABLE"
	// DocumentStatusDeleted permanently removes a source document from retrieval.
	DocumentStatusDeleted DocumentStatus = "DELETED"
)

// Document records SQL-authoritative metadata for one immutable source version.
type Document struct {
	Scope           tenant.Scope
	KnowledgeBaseID string
	ID              string
	Version         int
	ObjectKey       string
	ContentSHA256   []byte
	MIMEType        string
	Status          DocumentStatus
	IndexGeneration string
}

// Validate checks that Document has complete source-object metadata.
func (d Document) Validate() error {
	if err := d.Scope.Validate(); err != nil {
		return err
	}
	if d.KnowledgeBaseID == "" || d.ID == "" || d.ObjectKey == "" {
		return errors.New("knowledge base id, document id, and object key are required")
	}
	if d.Version < 0 {
		return errors.New("document version is invalid")
	}
	if len(d.ContentSHA256) != sha256.Size {
		return errors.New("document content sha256 is required")
	}
	if d.Status != DocumentStatusPending && d.Status != DocumentStatusAvailable && d.Status != DocumentStatusDeleted {
		return errors.New("document status is invalid")
	}
	if d.IndexGeneration == "" {
		return errors.New("index generation is required")
	}
	return nil
}

// Clone returns a copy that does not share the document hash buffer.
func (d Document) Clone() Document {
	cloned := d
	cloned.ContentSHA256 = bytes.Clone(d.ContentSHA256)
	return cloned
}

// ChunkStatus is the lifecycle state of a Qdrant-derived document chunk.
type ChunkStatus string

const (
	// ChunkStatusPending cannot be returned before its Qdrant write succeeds.
	ChunkStatusPending ChunkStatus = "PENDING"
	// ChunkStatusAvailable may be returned after SQL authorization.
	ChunkStatusAvailable ChunkStatus = "AVAILABLE"
	// ChunkStatusDeleted is excluded from every query.
	ChunkStatusDeleted ChunkStatus = "DELETED"
)

// Chunk records the SQL authority for one Qdrant payload.
type Chunk struct {
	Document Document
	ChunkID  string
	Status   ChunkStatus
}

// Validate checks that Chunk has an exact source version and vector identity.
func (c Chunk) Validate() error {
	if err := c.Document.Validate(); err != nil {
		return err
	}
	if c.ChunkID == "" {
		return errors.New("chunk id is required")
	}
	if c.Status != ChunkStatusPending && c.Status != ChunkStatusAvailable && c.Status != ChunkStatusDeleted {
		return errors.New("chunk status is invalid")
	}
	return nil
}

// IndexJobStatus identifies the durable indexing lifecycle for a source
// document version.
type IndexJobStatus string

const (
	// IndexJobPending is ready for a worker to process.
	IndexJobPending IndexJobStatus = "PENDING"
	// IndexJobRunning is leased by one worker.
	IndexJobRunning IndexJobStatus = "RUNNING"
	// IndexJobSucceeded has written and published every derived chunk.
	IndexJobSucceeded IndexJobStatus = "SUCCEEDED"
	// IndexJobFailed records a terminal failure after retryable attempts are exhausted.
	IndexJobFailed IndexJobStatus = "FAILED"
)

// IndexJob records one asynchronous source-document indexing operation. Its
// immutable config version fixes the Qdrant route and embedding settings.
type IndexJob struct {
	ID            string
	Document      Document
	ConfigVersion string
	Status        IndexJobStatus
	Attempt       int
	NextAttemptAt time.Time
	LeaseOwner    string
	LeaseUntil    time.Time
	RunToken      string
	LastError     string
}

// Validate checks the identity and lease state of IndexJob.
func (j IndexJob) Validate() error {
	if j.ID == "" || j.ConfigVersion == "" {
		return errors.New("knowledge index job identity is required")
	}
	if err := j.Document.Validate(); err != nil {
		return fmt.Errorf("knowledge index job document: %w", err)
	}
	if j.Document.Status != DocumentStatusAvailable {
		return errors.New("knowledge index job document must be available")
	}
	if j.Attempt < 0 {
		return errors.New("knowledge index job attempt must not be negative")
	}
	if j.Status != IndexJobPending && j.Status != IndexJobRunning &&
		j.Status != IndexJobSucceeded && j.Status != IndexJobFailed {
		return errors.New("knowledge index job status is invalid")
	}
	if j.LeaseOwner == "" && (!j.LeaseUntil.IsZero() || j.RunToken != "") {
		return errors.New("knowledge index job lease owner is required")
	}
	if j.LeaseOwner != "" && (j.LeaseUntil.IsZero() || j.RunToken == "") {
		return errors.New("knowledge index job lease is incomplete")
	}
	return nil
}

// Validate checks that Base has a complete authoritative identity.
func (b Base) Validate() error {
	if err := b.Scope.Validate(); err != nil {
		return err
	}
	if b.ID == "" {
		return errors.New("knowledge base id is required")
	}
	if b.Status != BaseStatusActive && b.Status != BaseStatusDeleted {
		return errors.New("knowledge base status is invalid")
	}
	return nil
}

// Validate checks that ChunkRef contains a complete authoritative identity.
func (r ChunkRef) Validate() error {
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	if r.ConfigVersion == "" {
		return errors.New("config version is required")
	}
	if r.KnowledgeBaseID == "" {
		return errors.New("knowledge base id is required")
	}
	if r.DocumentID == "" {
		return errors.New("document id is required")
	}
	if r.DocumentVersion == "" {
		return errors.New("document version is required")
	}
	if r.ChunkID == "" {
		return errors.New("chunk id is required")
	}
	if r.IndexGeneration == "" {
		return errors.New("index generation is required")
	}
	return nil
}

// Catalog authorizes a derived chunk against platform SQL metadata.
// Implementations must return false for deleted, pending, unbound, or
// cross-scope chunks.
type Catalog interface {
	AvailableKnowledgeChunk(context.Context, ChunkRef) (bool, error)
}

// Resolver resolves the framework Knowledge selected for one execution.
type Resolver interface {
	ResolveKnowledge(context.Context, worker.Execution) (frameworkknowledge.Knowledge, error)
}

// ScopedKnowledge adds mandatory platform filtering and SQL authorization to a
// framework Knowledge implementation. It never trusts a vector result alone.
type ScopedKnowledge struct {
	inner   frameworkknowledge.Knowledge
	catalog Catalog
	scope   tenant.Scope
	config  string
	baseIDs []string
}

// NewScopedKnowledge creates a Knowledge view limited to the supplied base IDs.
func NewScopedKnowledge(
	inner frameworkknowledge.Knowledge,
	catalog Catalog,
	scope tenant.Scope,
	configVersion string,
	knowledgeBaseIDs []string,
) (*ScopedKnowledge, error) {
	if inner == nil {
		return nil, errors.New("knowledge implementation is required")
	}
	if catalog == nil {
		return nil, errors.New("knowledge catalog is required")
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if configVersion == "" {
		return nil, errors.New("config version is required")
	}
	baseIDs := uniqueNonEmpty(knowledgeBaseIDs)
	return &ScopedKnowledge{
		inner:   inner,
		catalog: catalog,
		scope:   scope,
		config:  configVersion,
		baseIDs: baseIDs,
	}, nil
}

// Search retrieves candidates for all authorized knowledge bases, then
// filters every result through the SQL authority.
func (k *ScopedKnowledge) Search(
	ctx context.Context,
	req *frameworkknowledge.SearchRequest,
) (*frameworkknowledge.SearchResult, error) {
	if k == nil || k.inner == nil || k.catalog == nil {
		return nil, errors.New("scoped knowledge is not initialized")
	}
	if req == nil {
		return nil, errors.New("search request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(k.baseIDs) == 0 {
		return &frameworkknowledge.SearchResult{}, nil
	}

	searchLimit := scopedSearchLimit(req.MaxResults)
	var all []*frameworkknowledge.Result
	for {
		searchRequest := scopedRequest(req, k.scope, k.baseIDs)
		searchRequest.MaxResults = searchLimit
		result, err := k.inner.Search(ctx, searchRequest)
		if err != nil {
			return nil, fmt.Errorf("search scoped knowledge: %w", err)
		}
		all, err = k.authorizeCandidates(ctx, result)
		if err != nil {
			return nil, err
		}
		if req.MaxResults <= 0 || len(all) >= req.MaxResults ||
			result == nil || len(result.Documents) < searchLimit {
			break
		}
		nextLimit := nextScopedSearchLimit(searchLimit, req.MaxResults)
		if nextLimit == searchLimit {
			break
		}
		searchLimit = nextLimit
	}
	sort.SliceStable(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	if req.MaxResults > 0 && len(all) > req.MaxResults {
		all = all[:req.MaxResults]
	}
	return searchResult(all), nil
}

func (k *ScopedKnowledge) authorizeCandidates(
	ctx context.Context,
	result *frameworkknowledge.SearchResult,
) ([]*frameworkknowledge.Result, error) {
	all := make([]*frameworkknowledge.Result, 0)
	if result == nil {
		return all, nil
	}
	for _, candidate := range result.Documents {
		if candidate == nil || candidate.Document == nil {
			continue
		}
		baseID := metadataString(candidate.Document.Metadata, MetadataKnowledgeBaseID)
		if !slices.Contains(k.baseIDs, baseID) {
			continue
		}
		ref, ok := chunkRef(k.scope, k.config, baseID, candidate.Document)
		if !ok {
			continue
		}
		available, err := k.catalog.AvailableKnowledgeChunk(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("authorize knowledge chunk: %w", err)
		}
		if available {
			all = append(all, candidate)
		}
	}
	return all, nil
}

func scopedSearchLimit(maxResults int) int {
	if maxResults <= 0 {
		return maxResults
	}
	extra := maxResults * (scopedSearchInitialOverfetch - 1)
	if extra < 0 || extra > scopedSearchMaxOverfetch {
		extra = scopedSearchMaxOverfetch
	}
	maxInt := int(^uint(0) >> 1)
	if maxResults > maxInt-extra {
		return maxInt
	}
	return maxResults + extra
}

func nextScopedSearchLimit(current, maxResults int) int {
	maxLimit := scopedSearchMaxLimit(maxResults)
	if current <= 0 || current >= maxLimit {
		return current
	}
	next := current * 2
	if next < current || next > maxLimit {
		return maxLimit
	}
	return next
}

func scopedSearchMaxLimit(maxResults int) int {
	if maxResults <= 0 {
		return maxResults
	}
	maxInt := int(^uint(0) >> 1)
	if maxResults > maxInt-scopedSearchMaxOverfetch {
		return maxInt
	}
	return maxResults + scopedSearchMaxOverfetch
}

func scopedRequest(
	req *frameworkknowledge.SearchRequest,
	scope tenant.Scope,
	baseIDs []string,
) *frameworkknowledge.SearchRequest {
	cloned := *req
	filter := &frameworkknowledge.SearchFilter{}
	if req.SearchFilter != nil {
		filter.DocumentIDs = slices.Clone(req.SearchFilter.DocumentIDs)
		filter.FilterCondition = req.SearchFilter.FilterCondition
		filter.Metadata = cloneMetadata(req.SearchFilter.Metadata)
	}
	if filter.Metadata == nil {
		filter.Metadata = make(map[string]any)
	}
	filter.Metadata[MetadataTenantID] = scope.TenantID
	filter.Metadata[MetadataAppID] = scope.AppID
	delete(filter.Metadata, MetadataKnowledgeBaseID)
	baseValues := make([]any, len(baseIDs))
	for index, baseID := range baseIDs {
		baseValues[index] = baseID
	}
	baseCondition := searchfilter.In("metadata."+MetadataKnowledgeBaseID, baseValues...)
	if filter.FilterCondition == nil {
		filter.FilterCondition = baseCondition
	} else {
		filter.FilterCondition = searchfilter.And(filter.FilterCondition, baseCondition)
	}
	cloned.SearchFilter = filter
	return &cloned
}

func chunkRef(scope tenant.Scope, configVersion, baseID string, doc *document.Document) (ChunkRef, bool) {
	if doc == nil || doc.Metadata == nil {
		return ChunkRef{}, false
	}
	metadata := doc.Metadata
	if metadataString(metadata, MetadataTenantID) != scope.TenantID ||
		metadataString(metadata, MetadataAppID) != scope.AppID ||
		metadataString(metadata, MetadataKnowledgeBaseID) != baseID {
		return ChunkRef{}, false
	}
	ref := ChunkRef{
		Scope:           scope,
		ConfigVersion:   configVersion,
		KnowledgeBaseID: baseID,
		DocumentID:      metadataString(metadata, MetadataDocumentID),
		DocumentVersion: metadataString(metadata, MetadataDocumentVersion),
		ChunkID:         metadataString(metadata, MetadataChunkID),
		IndexGeneration: metadataString(metadata, MetadataIndexGeneration),
	}
	if err := ref.Validate(); err != nil {
		return ChunkRef{}, false
	}
	return ref, true
}

func searchResult(documents []*frameworkknowledge.Result) *frameworkknowledge.SearchResult {
	result := &frameworkknowledge.SearchResult{Documents: documents}
	if len(documents) == 0 {
		return result
	}
	result.Document = documents[0].Document
	result.Score = documents[0].Score
	result.Text = documents[0].Document.Content
	return result
}

func metadataString(metadata map[string]any, key string) string {
	value, ok := metadata[key]
	if !ok {
		return ""
	}
	text, _ := value.(string)
	return text
}

func cloneMetadata(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
