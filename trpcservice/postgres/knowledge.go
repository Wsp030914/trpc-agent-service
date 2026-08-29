package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
)

var _ platformknowledge.Catalog = (*Store)(nil)

// CreateKnowledgeBase creates a knowledge base owned by one tenant app.
func (s *Store) CreateKnowledgeBase(ctx context.Context, value platformknowledge.Base) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("knowledge base: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO platform.knowledge_base (tenant_id, app_id, knowledge_base_id, status)
VALUES ($1, $2, $3, $4)`,
		value.Scope.TenantID,
		value.Scope.AppID,
		value.ID,
		value.Status,
	); err != nil {
		return fmt.Errorf("create knowledge base: %w", err)
	}
	return nil
}

// CreateKnowledgeDocument persists metadata after the source object has been
// written to COS. Object paths are not used as authorization decisions.
func (s *Store) CreateKnowledgeDocument(ctx context.Context, value platformknowledge.Document) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("knowledge document: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO platform.knowledge_document (
    tenant_id, app_id, knowledge_base_id, document_id, version, object_key,
    content_sha256, mime_type, status, index_generation
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		value.Scope.TenantID,
		value.Scope.AppID,
		value.KnowledgeBaseID,
		value.ID,
		value.Version,
		value.ObjectKey,
		value.ContentSHA256,
		value.MIMEType,
		value.Status,
		value.IndexGeneration,
	); err != nil {
		return fmt.Errorf("create knowledge document: %w", err)
	}
	return nil
}

// CreateKnowledgeDocumentAndEnqueue persists source metadata and its index job
// in one transaction. The source object must have been written before calling
// this method and is never visible through platform retrieval until chunks are
// published separately.
func (s *Store) CreateKnowledgeDocumentAndEnqueue(
	ctx context.Context,
	value platformknowledge.Document,
	job platformknowledge.IndexJob,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("knowledge document: %w", err)
	}
	if err := job.Validate(); err != nil {
		return fmt.Errorf("knowledge index job: %w", err)
	}
	if !reflect.DeepEqual(job.Document, value) {
		return errors.New("knowledge index job document does not match metadata")
	}
	if job.Status != platformknowledge.IndexJobPending || job.LeaseOwner != "" || !job.LeaseUntil.IsZero() {
		return errors.New("new knowledge index job must be pending without a lease")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin create knowledge document: %w", err)
	}
	defer func() { rollback(tx) }()
	var activeConfigVersion string
	if err := tx.QueryRow(ctx, `
SELECT active_config_version
FROM platform.agent_app
WHERE tenant_id = $1 AND app_id = $2
FOR UPDATE`, value.Scope.TenantID, value.Scope.AppID).Scan(&activeConfigVersion); err != nil {
		return fmt.Errorf("lock knowledge import app: %w", resolveError("agent app", err))
	}
	if job.ConfigVersion != activeConfigVersion || job.BuildID != activeConfigVersion {
		return errors.New("knowledge import config is no longer active")
	}
	if err := upsertKnowledgeDocument(ctx, tx, value); err != nil {
		return err
	}
	if err := insertKnowledgeIndexJob(ctx, tx, job); err != nil {
		return err
	}
	if err := enqueuePendingKnowledgeGenerationJobs(ctx, tx, value); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit create knowledge document: %w", err)
	}
	return nil
}

func enqueuePendingKnowledgeGenerationJobs(
	ctx context.Context,
	tx pgx.Tx,
	value platformknowledge.Document,
) error {
	rows, err := tx.Query(ctx, `
SELECT build.build_id, build.config_version, build.index_generation
FROM platform.knowledge_generation_build AS build
JOIN platform.app_config_version AS config
  ON config.tenant_id = build.tenant_id
 AND config.app_id = build.app_id
 AND config.version = build.config_version
WHERE build.tenant_id = $1
  AND build.app_id = $2
  AND build.status = 'PENDING'
  AND config.knowledge_base_ids @> jsonb_build_array($3::text)`,
		value.Scope.TenantID, value.Scope.AppID, value.KnowledgeBaseID)
	if err != nil {
		return fmt.Errorf("list pending knowledge generation builds: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var buildID string
		var configVersion string
		var generation string
		if err := rows.Scan(&buildID, &configVersion, &generation); err != nil {
			return fmt.Errorf("read pending knowledge generation build: %w", err)
		}
		targetDocument := value.Clone()
		targetDocument.IndexGeneration = generation
		if err := insertKnowledgeIndexJob(ctx, tx, platformknowledge.IndexJob{
			ID:            uuid.NewString(),
			Document:      targetDocument,
			ConfigVersion: configVersion,
			BuildID:       buildID,
			Status:        platformknowledge.IndexJobPending,
		}); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate pending knowledge generation builds: %w", err)
	}
	return nil
}

// upsertKnowledgeDocument accepts a retry only when it describes the exact
// immutable source version already stored. A conflicting retry must not
// replace the object metadata selected by the original import.
func upsertKnowledgeDocument(ctx context.Context, db databaseExecutor, value platformknowledge.Document) error {
	tag, err := db.Exec(ctx, `
INSERT INTO platform.knowledge_document (
    tenant_id, app_id, knowledge_base_id, document_id, version, object_key,
    content_sha256, mime_type, status, index_generation
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (tenant_id, app_id, knowledge_base_id, document_id, version)
DO UPDATE SET updated_at = platform.knowledge_document.updated_at
WHERE platform.knowledge_document.object_key = EXCLUDED.object_key
  AND platform.knowledge_document.content_sha256 = EXCLUDED.content_sha256
  AND platform.knowledge_document.mime_type = EXCLUDED.mime_type
  AND platform.knowledge_document.status = EXCLUDED.status
  AND platform.knowledge_document.index_generation = EXCLUDED.index_generation`,
		value.Scope.TenantID,
		value.Scope.AppID,
		value.KnowledgeBaseID,
		value.ID,
		value.Version,
		value.ObjectKey,
		value.ContentSHA256,
		value.MIMEType,
		value.Status,
		value.IndexGeneration,
	)
	if err != nil {
		return fmt.Errorf("create knowledge document: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("knowledge document version already exists with different source metadata")
	}
	return nil
}

// CreateKnowledgeChunk records a derived chunk before its vector write. Only
// AVAILABLE chunks participate in SQL authorization.
func (s *Store) CreateKnowledgeChunk(ctx context.Context, value platformknowledge.Chunk) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("knowledge chunk: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO platform.knowledge_chunk (
    tenant_id, app_id, knowledge_base_id, document_id, document_version,
    index_generation, chunk_id, status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tenant_id, app_id, knowledge_base_id, document_id, document_version, index_generation, chunk_id)
DO NOTHING`,
		value.Document.Scope.TenantID,
		value.Document.Scope.AppID,
		value.Document.KnowledgeBaseID,
		value.Document.ID,
		value.Document.Version,
		value.Document.IndexGeneration,
		value.ChunkID,
		value.Status,
	); err != nil {
		return fmt.Errorf("create knowledge chunk: %w", err)
	}
	return nil
}

// MarkKnowledgeChunkAvailable publishes a chunk after Qdrant has accepted it.
func (s *Store) MarkKnowledgeChunkAvailable(ctx context.Context, value platformknowledge.Chunk) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := value.Validate(); err != nil {
		return fmt.Errorf("knowledge chunk: %w", err)
	}
	command, err := s.pool.Exec(ctx, `
UPDATE platform.knowledge_chunk
SET status = 'AVAILABLE', updated_at = now()
WHERE tenant_id = $1
  AND app_id = $2
  AND knowledge_base_id = $3
  AND document_id = $4
  AND document_version = $5
  AND index_generation = $6
	  AND chunk_id = $7
  AND status IN ('PENDING', 'AVAILABLE')`,
		value.Document.Scope.TenantID,
		value.Document.Scope.AppID,
		value.Document.KnowledgeBaseID,
		value.Document.ID,
		value.Document.Version,
		value.Document.IndexGeneration,
		value.ChunkID,
	)
	if err != nil {
		return fmt.Errorf("mark knowledge chunk available: %w", err)
	}
	if command.RowsAffected() == 0 {
		return fmt.Errorf("knowledge chunk: %w", ErrNotFound)
	}
	return nil
}

// AvailableKnowledgeChunk reports whether a Qdrant result still has an
// available document, chunk, active base, and immutable config binding.
func (s *Store) AvailableKnowledgeChunk(ctx context.Context, ref platformknowledge.ChunkRef) (bool, error) {
	if err := s.validate(); err != nil {
		return false, err
	}
	if err := ref.Validate(); err != nil {
		return false, err
	}
	version, err := strconv.Atoi(ref.DocumentVersion)
	if err != nil || version < 0 {
		return false, errors.New("document version is invalid")
	}
	var available bool
	err = s.pool.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM platform.knowledge_chunk AS chunk
    JOIN platform.knowledge_document AS document
      ON document.tenant_id = chunk.tenant_id
     AND document.app_id = chunk.app_id
     AND document.knowledge_base_id = chunk.knowledge_base_id
     AND document.document_id = chunk.document_id
     AND document.version = chunk.document_version
    JOIN platform.knowledge_base AS base
      ON base.tenant_id = chunk.tenant_id
     AND base.app_id = chunk.app_id
     AND base.knowledge_base_id = chunk.knowledge_base_id
    JOIN platform.app_config_version AS config
      ON config.tenant_id = chunk.tenant_id
     AND config.app_id = chunk.app_id
     AND config.version = $3
     AND config.status = 'PUBLISHED'
     AND config.knowledge_base_ids @> jsonb_build_array(chunk.knowledge_base_id)
    WHERE chunk.tenant_id = $1
      AND chunk.app_id = $2
      AND chunk.knowledge_base_id = $4
      AND chunk.document_id = $5
      AND chunk.document_version = $6
      AND chunk.index_generation = $7
      AND chunk.chunk_id = $8
      AND chunk.status = 'AVAILABLE'
      AND document.status = 'AVAILABLE'
      AND base.status = 'ACTIVE'
)`,
		ref.Scope.TenantID,
		ref.Scope.AppID,
		ref.ConfigVersion,
		ref.KnowledgeBaseID,
		ref.DocumentID,
		version,
		ref.IndexGeneration,
		ref.ChunkID,
	).Scan(&available)
	if err != nil {
		return false, fmt.Errorf("check available knowledge chunk: %w", err)
	}
	return available, nil
}
