// Package importer provides the platform-internal Knowledge source import flow.
package importer

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	knowledgecos "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const indexGenerationOption = "index_generation"

// SourceStore persists generated Knowledge source objects. Implementations
// must not accept caller-selected object paths.
type SourceStore interface {
	PutSource(context.Context, worker.Execution, platformknowledge.Document, []byte) error
	DeleteSource(context.Context, worker.Execution, platformknowledge.Document) error
}

// Repository persists source metadata and its durable indexing operation.
type Repository interface {
	CreateKnowledgeDocumentAndEnqueue(context.Context, platformknowledge.Document, platformknowledge.IndexJob) error
}

// Input identifies one internal source import. It is not an external upload
// request and does not contain an object path or authorization fields.
type Input struct {
	KnowledgeBaseID string
	DocumentID      string
	Version         int
	Content         []byte
	MIMEType        string
}

// Validate checks that Input can create one immutable source version.
func (i Input) Validate() error {
	if i.KnowledgeBaseID == "" || i.DocumentID == "" || i.Version < 0 {
		return errors.New("knowledge import identity is invalid")
	}
	return nil
}

// Importer writes a Knowledge source object, then atomically records its SQL
// metadata and index job. It owns compensation when the SQL transaction fails.
type Importer struct {
	sources    SourceStore
	repository Repository
}

// New creates an internal Knowledge importer.
func New(sources SourceStore, repository Repository) (*Importer, error) {
	if sources == nil {
		return nil, errors.New("knowledge source store is required")
	}
	if repository == nil {
		return nil, errors.New("knowledge repository is required")
	}
	return &Importer{sources: sources, repository: repository}, nil
}

// Import persists one internal source document and schedules asynchronous
// Qdrant indexing. The trusted execution fixes scope, config version and
// generation; callers cannot choose object keys or Tenant/App scope.
func (i *Importer) Import(
	ctx context.Context,
	exec worker.Execution,
	input Input,
) (platformknowledge.Document, error) {
	if i == nil || i.sources == nil || i.repository == nil {
		return platformknowledge.Document{}, errors.New("knowledge importer is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return platformknowledge.Document{}, err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return platformknowledge.Document{}, err
	}
	if err := input.Validate(); err != nil {
		return platformknowledge.Document{}, err
	}
	ref := exec.Config.BackendConfig.Knowledge
	if ref.IsZero() {
		return platformknowledge.Document{}, errors.New("knowledge backend is not configured")
	}
	if exec.Config.BackendConfig.Artifact.IsZero() {
		return platformknowledge.Document{}, errors.New("artifact cos backend is required for knowledge source objects")
	}
	generation := ref.Options[indexGenerationOption]
	if generation == "" {
		return platformknowledge.Document{}, errors.New("knowledge backend index_generation is required")
	}
	if !contains(exec.Config.KnowledgeBaseIDs, input.KnowledgeBaseID) {
		return platformknowledge.Document{}, errors.New("knowledge base is not bound by config")
	}
	document, err := knowledgecos.NewDocument(
		exec.Tenant.Scope(),
		input.KnowledgeBaseID,
		input.DocumentID,
		input.Version,
		input.Content,
		input.MIMEType,
		generation,
	)
	if err != nil {
		return platformknowledge.Document{}, err
	}
	if err := i.sources.PutSource(ctx, exec, document, input.Content); err != nil {
		return platformknowledge.Document{}, err
	}
	job := platformknowledge.IndexJob{
		ID:            uuid.NewString(),
		Document:      document,
		ConfigVersion: exec.Tenant.ConfigVersion,
		BuildID:       exec.Tenant.ConfigVersion,
		Status:        platformknowledge.IndexJobPending,
	}
	if err := i.repository.CreateKnowledgeDocumentAndEnqueue(ctx, document, job); err != nil {
		cleanupErr := i.sources.DeleteSource(context.WithoutCancel(ctx), exec, document)
		if cleanupErr != nil {
			return platformknowledge.Document{}, errors.Join(
				fmt.Errorf("persist knowledge source metadata: %w", err),
				fmt.Errorf("compensate knowledge source: %w", cleanupErr),
			)
		}
		return platformknowledge.Document{}, fmt.Errorf("persist knowledge source metadata: %w", err)
	}
	return document, nil
}

func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

var _ SourceStore = (*knowledgecos.Resolver)(nil)
