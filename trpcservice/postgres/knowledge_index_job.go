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
    index_generation, config_version, status, last_error
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (tenant_id, app_id, knowledge_base_id, document_id, document_version,
             index_generation, config_version)
DO NOTHING`,
		job.ID,
		job.Document.Scope.TenantID,
		job.Document.Scope.AppID,
		job.Document.KnowledgeBaseID,
		job.Document.ID,
		job.Document.Version,
		job.Document.IndexGeneration,
		job.ConfigVersion,
		job.Status,
		job.LastError,
	)
	if err != nil {
		return fmt.Errorf("enqueue knowledge index: %w", err)
	}
	return nil
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
       document.mime_type, document.status, claimed.index_generation, claimed.config_version,
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
