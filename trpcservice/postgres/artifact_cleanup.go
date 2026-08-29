package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
)

// EnqueueArtifactCleanup records one unreachable object for asynchronous
// deletion. Repeated failures for the same immutable object share one active
// cleanup record.
func (s *Store) EnqueueArtifactCleanup(ctx context.Context, record platformartifact.CleanupRecord) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Status != platformartifact.CleanupPending || record.LeaseOwner != "" || !record.LeaseUntil.IsZero() {
		return errors.New("new artifact cleanup must be pending without a lease")
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO platform.artifact_cleanup (
    cleanup_id, tenant_id, app_id, config_version, session_principal_id,
    session_id, filename, object_key, version, status, last_error
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (tenant_id, app_id, config_version, session_principal_id, session_id, object_key, version)
    WHERE status IN ('PENDING', 'RUNNING')
DO UPDATE SET
    last_error = EXCLUDED.last_error,
    updated_at = clock_timestamp()`,
		record.ID,
		record.TenantID,
		record.AppID,
		record.ConfigVersion,
		record.SessionPrincipalID,
		record.SessionID,
		record.Filename,
		record.ObjectKey,
		record.Version,
		record.Status,
		record.LastError,
	)
	if err != nil {
		return fmt.Errorf("enqueue artifact cleanup: %w", err)
	}
	return nil
}

func scanArtifactCleanup(scanner interface{ Scan(...any) error }) (platformartifact.CleanupRecord, error) {
	var record platformartifact.CleanupRecord
	if err := scanner.Scan(
		&record.ID,
		&record.TenantID,
		&record.AppID,
		&record.ConfigVersion,
		&record.SessionPrincipalID,
		&record.SessionID,
		&record.Filename,
		&record.ObjectKey,
		&record.Version,
		&record.Status,
		&record.Attempt,
		&record.NextAttemptAt,
		&record.LeaseOwner,
		&record.LeaseUntil,
		&record.RunToken,
		&record.LastError,
	); err != nil {
		return platformartifact.CleanupRecord{}, err
	}
	if err := record.Validate(); err != nil {
		return platformartifact.CleanupRecord{}, fmt.Errorf("read artifact cleanup: %w", err)
	}
	return record, nil
}

// ClaimNextArtifactCleanup leases one due cleanup record for owner. It returns
// found=false when no cleanup is ready. A new run token rejects stale workers.
func (s *Store) ClaimNextArtifactCleanup(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
) (record platformartifact.CleanupRecord, found bool, err error) {
	if err := s.validate(); err != nil {
		return platformartifact.CleanupRecord{}, false, err
	}
	if owner == "" || leaseDuration <= 0 {
		return platformartifact.CleanupRecord{}, false, errors.New("artifact cleanup owner and positive lease duration are required")
	}
	row := s.pool.QueryRow(ctx, `
WITH candidate AS (
    SELECT cleanup_id
    FROM platform.artifact_cleanup
    WHERE (status = 'PENDING' AND next_attempt_at <= clock_timestamp())
       OR (status = 'RUNNING' AND lease_until <= clock_timestamp())
    ORDER BY next_attempt_at, cleanup_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE platform.artifact_cleanup AS cleanup
SET status = 'RUNNING',
    attempt = cleanup.attempt + 1,
    lease_owner = $1,
    lease_until = clock_timestamp() + $2::interval,
    run_token = $3,
    updated_at = clock_timestamp()
FROM candidate
WHERE cleanup.cleanup_id = candidate.cleanup_id
RETURNING cleanup.cleanup_id, cleanup.tenant_id, cleanup.app_id, cleanup.config_version,
          cleanup.session_principal_id, cleanup.session_id, cleanup.filename,
          cleanup.object_key, cleanup.version, cleanup.status, cleanup.attempt,
          cleanup.next_attempt_at, cleanup.lease_owner, cleanup.lease_until,
          cleanup.run_token, cleanup.last_error`,
		owner, intervalLiteral(leaseDuration), uuid.NewString())
	record, err = scanArtifactCleanup(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return platformartifact.CleanupRecord{}, false, nil
	}
	if err != nil {
		return platformartifact.CleanupRecord{}, false, fmt.Errorf("claim artifact cleanup: %w", err)
	}
	return record, true, nil
}

// CompleteArtifactCleanup records successful deletion for the current lease
// holder and retains the cleanup row as an audit record.
func (s *Store) CompleteArtifactCleanup(ctx context.Context, record platformartifact.CleanupRecord) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Status != platformartifact.CleanupRunning {
		return errors.New("artifact cleanup must be running to complete")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.artifact_cleanup
SET status = 'SUCCEEDED', lease_owner = NULL, lease_until = NULL, run_token = NULL,
    last_error = '', updated_at = clock_timestamp()
WHERE cleanup_id = $1 AND status = 'RUNNING' AND lease_owner = $2 AND run_token = $3
  AND lease_until > clock_timestamp()`, record.ID, record.LeaseOwner, record.RunToken)
	if err != nil {
		return fmt.Errorf("complete artifact cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete artifact cleanup: %w", platformartifact.ErrCleanupLeaseLost)
	}
	return nil
}

// RetryArtifactCleanup releases the current cleanup lease and schedules the
// next attempt after delay. The stale owner cannot schedule a successor.
func (s *Store) RetryArtifactCleanup(
	ctx context.Context,
	record platformartifact.CleanupRecord,
	delay time.Duration,
	cause error,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Status != platformartifact.CleanupRunning || delay <= 0 || cause == nil {
		return errors.New("artifact cleanup retry is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.artifact_cleanup
SET status = 'PENDING', next_attempt_at = clock_timestamp() + $4::interval,
    lease_owner = NULL, lease_until = NULL, run_token = NULL, last_error = $5,
    updated_at = clock_timestamp()
WHERE cleanup_id = $1 AND status = 'RUNNING' AND lease_owner = $2 AND run_token = $3
  AND lease_until > clock_timestamp()`,
		record.ID, record.LeaseOwner, record.RunToken, intervalLiteral(delay), cause.Error())
	if err != nil {
		return fmt.Errorf("retry artifact cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retry artifact cleanup: %w", platformartifact.ErrCleanupLeaseLost)
	}
	return nil
}

var _ platformartifact.MetadataStore = (*Store)(nil)
