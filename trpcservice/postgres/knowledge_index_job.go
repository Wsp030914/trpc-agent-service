package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// EnqueueKnowledgeIndex records a source document version for asynchronous
// Qdrant indexing. Repeated requests reuse the immutable build result.
func (s *Store) EnqueueKnowledgeIndex(ctx context.Context, job platformknowledge.IndexJob) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := job.Validate(); err != nil {
		return err
	}
	if job.Status != platformknowledge.IndexJobPending || job.LeaseOwner != "" || !job.LeaseUntil.IsZero() {
		return errors.New("new knowledge index job must be pending without a lease")
	}
	if err := insertKnowledgeIndexJob(ctx, s.pool, job); err != nil {
		return err
	}
	return nil
}

func insertKnowledgeIndexJob(ctx context.Context, db databaseExecutor, job platformknowledge.IndexJob) error {
	job.LastError = platformlog.SafeError(errors.New(job.LastError))
	_, err := db.Exec(ctx, `
INSERT INTO platform.knowledge_index_job (
    job_id, tenant_id, app_id, knowledge_base_id, document_id, document_version,
    index_generation, config_version, build_id, status, last_error
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (tenant_id, app_id, knowledge_base_id, document_id, document_version,
             index_generation, config_version, build_id)
DO NOTHING`,
		job.ID,
		job.Document.Scope.TenantID,
		job.Document.Scope.AppID,
		job.Document.KnowledgeBaseID,
		job.Document.ID,
		job.Document.Version,
		job.Document.IndexGeneration,
		job.ConfigVersion,
		job.BuildID,
		job.Status,
		job.LastError,
	)
	if err != nil {
		return fmt.Errorf("enqueue knowledge index: %w", err)
	}
	return nil
}

func enqueueKnowledgeGeneration(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.Scope,
	configVersion string,
	buildID string,
	baseIDs []string,
	generation string,
) (int, error) {
	if len(baseIDs) == 0 || generation == "" || buildID == "" {
		return 0, nil
	}
	rows, err := tx.Query(ctx, `
SELECT document.knowledge_base_id, document.document_id, document.version,
       document.object_key, document.content_sha256, document.mime_type, document.status
FROM platform.knowledge_document AS document
WHERE document.tenant_id = $1
  AND document.app_id = $2
  AND document.knowledge_base_id = ANY($3)
  AND document.status = 'AVAILABLE'
  AND NOT EXISTS (
      SELECT 1
      FROM platform.knowledge_index_job AS job
      WHERE job.tenant_id = document.tenant_id
        AND job.app_id = document.app_id
        AND job.knowledge_base_id = document.knowledge_base_id
        AND job.document_id = document.document_id
        AND job.document_version = document.version
        AND job.index_generation = $4
        AND job.config_version = $5
        AND job.build_id = $6
  )`, scope.TenantID, scope.AppID, baseIDs, generation, configVersion, buildID)
	if err != nil {
		return 0, fmt.Errorf("list knowledge generation sources: %w", err)
	}
	defer rows.Close()
	documents := make([]platformknowledge.Document, 0)
	for rows.Next() {
		document := platformknowledge.Document{
			Scope:           scope,
			Status:          platformknowledge.DocumentStatusAvailable,
			IndexGeneration: generation,
		}
		if err := rows.Scan(
			&document.KnowledgeBaseID,
			&document.ID,
			&document.Version,
			&document.ObjectKey,
			&document.ContentSHA256,
			&document.MIMEType,
			&document.Status,
		); err != nil {
			return 0, fmt.Errorf("read knowledge generation source: %w", err)
		}
		documents = append(documents, document)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate knowledge generation sources: %w", err)
	}
	rows.Close()
	for _, document := range documents {
		job := platformknowledge.IndexJob{
			ID:            uuid.NewString(),
			Document:      document,
			ConfigVersion: configVersion,
			BuildID:       buildID,
			Status:        platformknowledge.IndexJobPending,
		}
		if err := insertKnowledgeIndexJob(ctx, tx, job); err != nil {
			return 0, err
		}
	}
	return len(documents), nil
}

func knowledgeGenerationReady(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.Scope,
	configVersion string,
	buildID string,
	baseIDs []string,
	generation string,
) (bool, error) {
	if len(baseIDs) == 0 || generation == "" || buildID == "" {
		return true, nil
	}
	var ready bool
	err := tx.QueryRow(ctx, `
SELECT NOT EXISTS (
    SELECT 1
    FROM platform.knowledge_document AS document
    WHERE document.tenant_id = $1
      AND document.app_id = $2
      AND document.knowledge_base_id = ANY($3)
      AND document.status = 'AVAILABLE'
      AND NOT EXISTS (
          SELECT 1
          FROM platform.knowledge_index_job AS job
          WHERE job.tenant_id = document.tenant_id
            AND job.app_id = document.app_id
            AND job.knowledge_base_id = document.knowledge_base_id
            AND job.document_id = document.document_id
            AND job.document_version = document.version
            AND job.index_generation = $4
            AND job.config_version = $5
            AND job.build_id = $6
            AND job.status = 'SUCCEEDED'
      )
)`, scope.TenantID, scope.AppID, baseIDs, generation, configVersion, buildID).Scan(&ready)
	if err != nil {
		return false, fmt.Errorf("check knowledge generation readiness: %w", err)
	}
	return ready, nil
}

func ensureKnowledgeGenerationBuild(
	ctx context.Context,
	tx pgx.Tx,
	scope tenant.Scope,
	configVersion string,
	generation string,
) (string, platformknowledge.IndexJobStatus, error) {
	var buildID string
	var storedGeneration string
	var status platformknowledge.IndexJobStatus
	err := tx.QueryRow(ctx, `
SELECT build_id, index_generation, status
FROM platform.knowledge_generation_build
WHERE tenant_id = $1 AND app_id = $2 AND config_version = $3
FOR UPDATE`, scope.TenantID, scope.AppID, configVersion).Scan(&buildID, &storedGeneration, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		buildID = uuid.NewString()
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.knowledge_generation_build (
    build_id, tenant_id, app_id, config_version, index_generation, status
) VALUES ($1, $2, $3, $4, $5, 'PENDING')`,
			buildID, scope.TenantID, scope.AppID, configVersion, generation); err != nil {
			return "", "", fmt.Errorf("create knowledge generation build: %w", err)
		}
		return buildID, platformknowledge.IndexJobPending, nil
	}
	if err != nil {
		return "", "", fmt.Errorf("lock knowledge generation build: %w", err)
	}
	if storedGeneration != generation {
		return "", "", errors.New("knowledge generation build does not match config")
	}
	return buildID, status, nil
}

func failKnowledgeGenerationBuild(
	ctx context.Context,
	tx pgx.Tx,
	buildID string,
	cause string,
) error {
	if cause == "" {
		cause = "knowledge index job failed"
	}
	cause = platformlog.SafeError(errors.New(cause))
	if _, err := tx.Exec(ctx, `
UPDATE platform.knowledge_generation_build
SET status = 'FAILED', last_error = $2, updated_at = clock_timestamp()
WHERE build_id = $1 AND status = 'PENDING'`, buildID, cause); err != nil {
		return fmt.Errorf("fail knowledge generation build: %w", err)
	}
	return nil
}

func completeKnowledgeGenerationBuild(ctx context.Context, tx pgx.Tx, buildID string) error {
	if _, err := tx.Exec(ctx, `
UPDATE platform.knowledge_generation_build
SET status = 'SUCCEEDED', last_error = '', updated_at = clock_timestamp()
WHERE build_id = $1 AND status = 'PENDING'`, buildID); err != nil {
		return fmt.Errorf("complete knowledge generation build: %w", err)
	}
	return nil
}

func failedKnowledgeGenerationJob(
	ctx context.Context,
	tx pgx.Tx,
	buildID string,
) (string, bool, error) {
	var cause string
	err := tx.QueryRow(ctx, `
SELECT last_error
FROM platform.knowledge_index_job
WHERE build_id = $1 AND status = 'FAILED'
ORDER BY updated_at, job_id
LIMIT 1`, buildID).Scan(&cause)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("read failed knowledge index job: %w", err)
	}
	return cause, true, nil
}

func scanKnowledgeIndexJob(scanner interface{ Scan(...any) error }) (platformknowledge.IndexJob, error) {
	var job platformknowledge.IndexJob
	if err := scanner.Scan(
		&job.ID,
		&job.Document.Scope.TenantID,
		&job.Document.Scope.AppID,
		&job.Document.KnowledgeBaseID,
		&job.Document.ID,
		&job.Document.Version,
		&job.Document.ObjectKey,
		&job.Document.ContentSHA256,
		&job.Document.MIMEType,
		&job.Document.Status,
		&job.Document.IndexGeneration,
		&job.ConfigVersion,
		&job.BuildID,
		&job.Status,
		&job.Attempt,
		&job.NextAttemptAt,
		&job.LeaseOwner,
		&job.LeaseUntil,
		&job.RunToken,
		&job.LastError,
	); err != nil {
		return platformknowledge.IndexJob{}, err
	}
	if err := job.Validate(); err != nil {
		return platformknowledge.IndexJob{}, fmt.Errorf("read knowledge index job: %w", err)
	}
	return job, nil
}

// ClaimNextKnowledgeIndex leases one due index job for owner. It returns
// found=false when no job is ready. A new run token rejects stale workers.
func (s *Store) ClaimNextKnowledgeIndex(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
) (job platformknowledge.IndexJob, found bool, err error) {
	if err := s.validate(); err != nil {
		return platformknowledge.IndexJob{}, false, err
	}
	if owner == "" || leaseDuration <= 0 {
		return platformknowledge.IndexJob{}, false, errors.New("knowledge index owner and positive lease duration are required")
	}
	row := s.pool.QueryRow(ctx, `
WITH candidate AS (
    SELECT job_id
    FROM platform.knowledge_index_job
    WHERE (status = 'PENDING' AND next_attempt_at <= clock_timestamp())
       OR (status = 'RUNNING' AND lease_until <= clock_timestamp())
    ORDER BY next_attempt_at, job_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
), claimed AS (
    UPDATE platform.knowledge_index_job AS job
    SET status = 'RUNNING',
        attempt = job.attempt + 1,
        lease_owner = $1,
        lease_until = clock_timestamp() + $2::interval,
        run_token = $3,
        updated_at = clock_timestamp()
    FROM candidate
    WHERE job.job_id = candidate.job_id
    RETURNING job.*
)
SELECT claimed.job_id, document.tenant_id, document.app_id, document.knowledge_base_id,
       document.document_id, document.version, document.object_key, document.content_sha256,
       document.mime_type, document.status, claimed.index_generation, claimed.config_version, claimed.build_id,
       claimed.status, claimed.attempt, claimed.next_attempt_at, claimed.lease_owner,
       claimed.lease_until, claimed.run_token, claimed.last_error
FROM claimed
JOIN platform.knowledge_document AS document
  ON document.tenant_id = claimed.tenant_id
 AND document.app_id = claimed.app_id
 AND document.knowledge_base_id = claimed.knowledge_base_id
 AND document.document_id = claimed.document_id
 AND document.version = claimed.document_version`,
		owner, intervalLiteral(leaseDuration), uuid.NewString())
	job, err = scanKnowledgeIndexJob(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return platformknowledge.IndexJob{}, false, nil
	}
	if err != nil {
		return platformknowledge.IndexJob{}, false, fmt.Errorf("claim knowledge index: %w", err)
	}
	return job, true, nil
}

// CompleteKnowledgeIndex records successful Qdrant publication for the current
// lease holder and retains the job as an audit record.
func (s *Store) CompleteKnowledgeIndex(ctx context.Context, job platformknowledge.IndexJob) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := job.Validate(); err != nil {
		return err
	}
	if job.Status != platformknowledge.IndexJobRunning {
		return errors.New("knowledge index job must be running to complete")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.knowledge_index_job
SET status = 'SUCCEEDED', lease_owner = NULL, lease_until = NULL, run_token = NULL,
    last_error = '', updated_at = clock_timestamp()
WHERE job_id = $1 AND status = 'RUNNING' AND lease_owner = $2 AND run_token = $3
  AND lease_until > clock_timestamp()`, job.ID, job.LeaseOwner, job.RunToken)
	if err != nil {
		return fmt.Errorf("complete knowledge index: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete knowledge index: %w", platformknowledge.ErrIndexLeaseLost)
	}
	return nil
}

// RenewKnowledgeIndex extends a current job lease. It rejects a worker that
// has been superseded or whose lease already expired.
func (s *Store) RenewKnowledgeIndex(
	ctx context.Context,
	job platformknowledge.IndexJob,
	leaseDuration time.Duration,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := job.Validate(); err != nil {
		return err
	}
	if job.Status != platformknowledge.IndexJobRunning || leaseDuration <= 0 {
		return errors.New("knowledge index lease renewal is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.knowledge_index_job
SET lease_until = clock_timestamp() + $4::interval, updated_at = clock_timestamp()
WHERE job_id = $1 AND status = 'RUNNING' AND lease_owner = $2 AND run_token = $3
  AND lease_until > clock_timestamp()`,
		job.ID, job.LeaseOwner, job.RunToken, intervalLiteral(leaseDuration))
	if err != nil {
		return fmt.Errorf("renew knowledge index: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return platformknowledge.ErrIndexLeaseLost
	}
	return nil
}

// RetryKnowledgeIndex releases the current job lease and schedules its next
// attempt. The stale owner cannot schedule a successor.
func (s *Store) RetryKnowledgeIndex(
	ctx context.Context,
	job platformknowledge.IndexJob,
	delay time.Duration,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := job.Validate(); err != nil {
		return err
	}
	if job.Status != platformknowledge.IndexJobRunning || delay <= 0 || cause == nil {
		return errors.New("knowledge index retry is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.knowledge_index_job
SET status = 'PENDING', next_attempt_at = clock_timestamp() + $4::interval,
    lease_owner = NULL, lease_until = NULL, run_token = NULL, last_error = $5,
    updated_at = clock_timestamp()
WHERE job_id = $1 AND status = 'RUNNING' AND lease_owner = $2 AND run_token = $3
  AND lease_until > clock_timestamp()`,
		job.ID, job.LeaseOwner, job.RunToken, intervalLiteral(delay), platformlog.SafeError(cause))
	if err != nil {
		return fmt.Errorf("retry knowledge index: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retry knowledge index: %w", platformknowledge.ErrIndexLeaseLost)
	}
	return nil
}

// FailKnowledgeIndex records a terminal indexing failure for the current
// lease holder. A failed job is retained for diagnosis and is not claimable.
func (s *Store) FailKnowledgeIndex(
	ctx context.Context,
	job platformknowledge.IndexJob,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := job.Validate(); err != nil {
		return err
	}
	if job.Status != platformknowledge.IndexJobRunning || cause == nil {
		return errors.New("knowledge index failure is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.knowledge_index_job
SET status = 'FAILED', lease_owner = NULL, lease_until = NULL, run_token = NULL,
    last_error = $4, updated_at = clock_timestamp()
WHERE job_id = $1 AND status = 'RUNNING' AND lease_owner = $2 AND run_token = $3
	  AND lease_until > clock_timestamp()`, job.ID, job.LeaseOwner, job.RunToken, platformlog.SafeError(cause))
	if err != nil {
		return fmt.Errorf("fail knowledge index: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("fail knowledge index: %w", platformknowledge.ErrIndexLeaseLost)
	}
	return nil
}
