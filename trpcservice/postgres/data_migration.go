package postgres

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// CreateDataMigration records a validated PENDING migration. The source must
// be active, the target must be published, and only backend configuration may
// differ between the two immutable versions.
func (s *Store) CreateDataMigration(ctx context.Context, record migration.Record) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Status != migration.StatusPending || record.LeaseOwner != "" || !record.DrainDeadline.IsZero() {
		return errors.New("new data migration must be pending without a lease or drain deadline")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin data migration: %w", err)
	}
	defer func() { rollback(tx) }()
	app, err := lockAgentApp(ctx, tx, record.TenantID, record.AppID)
	if err != nil {
		return err
	}
	if app.ActiveConfigVersion != record.SourceConfigVersion {
		return errors.New("data migration source config is not active")
	}
	source, err := resolveAppConfigFrom(ctx, tx, record.TenantID, record.AppID, record.SourceConfigVersion)
	if err != nil {
		return err
	}
	target, err := resolveAppConfigFrom(ctx, tx, record.TenantID, record.AppID, record.TargetConfigVersion)
	if err != nil {
		return err
	}
	if sameAuthoritativeBackends(source.BackendConfig, target.BackendConfig) {
		return errors.New("data migration target does not change authoritative backends")
	}
	if err := validateSupportedDataMigration(source.BackendConfig, target.BackendConfig); err != nil {
		return err
	}
	if !sameMigrationBehavior(source, target) {
		return errors.New("data migration target changes behavior outside backend_config")
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.data_migration (
    migration_id, tenant_id, app_id, source_config_version, target_config_version, status
) VALUES ($1, $2, $3, $4, $5, $6)`,
		record.ID,
		record.TenantID,
		record.AppID,
		record.SourceConfigVersion,
		record.TargetConfigVersion,
		record.Status,
	); err != nil {
		return fmt.Errorf("insert data migration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit data migration: %w", err)
	}
	return nil
}

// BeginDataMigration enters DRAINING with a durable owner token. The caller
// must finish or fail the record before another migration can begin.
func (s *Store) BeginDataMigration(
	ctx context.Context,
	tenantID, appID, migrationID, owner string,
	drainDeadline time.Time,
	leaseDuration time.Duration,
) (migration.Record, error) {
	if err := s.validate(); err != nil {
		return migration.Record{}, err
	}
	if tenantID == "" || appID == "" || migrationID == "" || owner == "" {
		return migration.Record{}, errors.New("data migration scope, id, and owner are required")
	}
	if leaseDuration <= 0 {
		return migration.Record{}, errors.New("data migration lease duration must be positive")
	}
	if !drainDeadline.After(time.Now()) {
		return migration.Record{}, errors.New("data migration drain deadline must be in the future")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return migration.Record{}, fmt.Errorf("begin data migration drain: %w", err)
	}
	defer func() { rollback(tx) }()
	app, err := lockAgentApp(ctx, tx, tenantID, appID)
	if err != nil {
		return migration.Record{}, err
	}
	token := uuid.NewString()
	var record migration.Record
	err = tx.QueryRow(ctx, `
UPDATE platform.data_migration
SET status = 'DRAINING', lease_owner = $4, lease_until = clock_timestamp() + $5::interval,
    run_token = $6, drain_deadline = $7, updated_at = clock_timestamp()
WHERE migration_id = $1 AND tenant_id = $2 AND app_id = $3 AND status = 'PENDING'
  AND source_config_version = $8
RETURNING migration_id, tenant_id, app_id, source_config_version, target_config_version,
          status, lease_owner, lease_until, run_token, drain_deadline,
          failure_reason`,
		migrationID, tenantID, appID, owner, intervalLiteral(leaseDuration), token, drainDeadline.UTC(), app.ActiveConfigVersion,
	).Scan(
		&record.ID,
		&record.TenantID,
		&record.AppID,
		&record.SourceConfigVersion,
		&record.TargetConfigVersion,
		&record.Status,
		&record.LeaseOwner,
		&record.LeaseUntil,
		&record.RunToken,
		&record.DrainDeadline,
		&record.FailureReason,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return migration.Record{}, fmt.Errorf("begin data migration: %w", ErrNotFound)
	}
	if err != nil {
		return migration.Record{}, fmt.Errorf("begin data migration: %w", err)
	}
	if err := record.Validate(); err != nil {
		return migration.Record{}, fmt.Errorf("started data migration: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return migration.Record{}, fmt.Errorf("commit data migration drain: %w", err)
	}
	return record, nil
}

// ListOwnedDataMigrations returns the active migrations currently leased by
// owner. It is used by a worker to resume work it already owns before
// attempting to take over an expired migration.
func (s *Store) ListOwnedDataMigrations(
	ctx context.Context,
	owner string,
) ([]migration.Record, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("data migration owner is required")
	}
	rows, err := s.pool.Query(ctx, `
SELECT migration_id, tenant_id, app_id, source_config_version, target_config_version,
       status, lease_owner, lease_until, run_token, drain_deadline,
       failure_reason
FROM platform.data_migration
WHERE lease_owner = $1
  AND status IN ('DRAINING', 'COPYING', 'VERIFYING')
  AND lease_until > clock_timestamp()
ORDER BY created_at, migration_id`, owner)
	if err != nil {
		return nil, fmt.Errorf("list owned data migrations: %w", err)
	}
	defer rows.Close()
	records := make([]migration.Record, 0)
	for rows.Next() {
		record, err := scanDataMigrationRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate owned data migrations: %w", err)
	}
	return records, nil
}

// ClaimNextDataMigration atomically claims one expired active migration for
// owner. It returns found=false when no migration needs takeover.
func (s *Store) ClaimNextDataMigration(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
) (record migration.Record, found bool, err error) {
	if err := s.validate(); err != nil {
		return migration.Record{}, false, err
	}
	if owner == "" || leaseDuration <= 0 {
		return migration.Record{}, false, errors.New("data migration owner and positive lease duration are required")
	}
	token := uuid.NewString()
	row := s.pool.QueryRow(ctx, `
WITH candidate AS (
    SELECT migration_id
    FROM platform.data_migration
    WHERE status IN ('DRAINING', 'COPYING', 'VERIFYING')
      AND (lease_until IS NULL OR lease_until <= clock_timestamp())
    ORDER BY updated_at, migration_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
UPDATE platform.data_migration AS migration
SET lease_owner = $1,
    lease_until = clock_timestamp() + $2::interval,
    run_token = $3,
    updated_at = clock_timestamp()
FROM candidate
WHERE migration.migration_id = candidate.migration_id
RETURNING migration.migration_id, migration.tenant_id, migration.app_id,
          migration.source_config_version, migration.target_config_version,
          migration.status, migration.lease_owner, migration.lease_until,
          migration.run_token, migration.drain_deadline, migration.failure_reason`, owner, intervalLiteral(leaseDuration), token)
	record, err = scanDataMigrationRecord(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return migration.Record{}, false, nil
	}
	if err != nil {
		return migration.Record{}, false, fmt.Errorf("claim next data migration: %w", err)
	}
	return record, true, nil
}

// RenewDataMigration extends the current worker's lease. A replaced or expired
// token is rejected so old workers cannot overwrite their successor's state.
func (s *Store) RenewDataMigration(
	ctx context.Context,
	record migration.Record,
	leaseDuration time.Duration,
) (migration.Record, error) {
	if err := s.validate(); err != nil {
		return migration.Record{}, err
	}
	if err := record.Validate(); err != nil {
		return migration.Record{}, err
	}
	if record.IsTerminal() || leaseDuration <= 0 {
		return migration.Record{}, errors.New("data migration lease renewal is invalid")
	}
	var until time.Time
	err := s.pool.QueryRow(ctx, `
UPDATE platform.data_migration
SET lease_until = clock_timestamp() + $6::interval, updated_at = clock_timestamp()
WHERE migration_id = $1 AND tenant_id = $2 AND app_id = $3
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()
RETURNING lease_until`,
		record.ID, record.TenantID, record.AppID, record.LeaseOwner, record.RunToken, intervalLiteral(leaseDuration),
	).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return migration.Record{}, fmt.Errorf("renew data migration: %w", migration.ErrLeaseLost)
	}
	if err != nil {
		return migration.Record{}, fmt.Errorf("renew data migration: %w", err)
	}
	record.LeaseUntil = until.UTC()
	return record, nil
}

// AdvanceDataMigration changes the current lease owner's state. Succeeding a
// migration activates its target config in the same transaction.
func (s *Store) AdvanceDataMigration(ctx context.Context, record migration.Record, next migration.Status) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if !record.CanTransition(next) {
		return errors.New("data migration state transition is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin data migration transition: %w", err)
	}
	defer func() { rollback(tx) }()
	if next == migration.StatusSucceeded {
		app, err := lockAgentApp(ctx, tx, record.TenantID, record.AppID)
		if err != nil {
			return err
		}
		if app.ActiveConfigVersion != record.SourceConfigVersion {
			return errors.New("data migration source config is no longer active")
		}
		if _, err := resolveAppConfigFrom(ctx, tx, record.TenantID, record.AppID, record.TargetConfigVersion); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE platform.agent_app
SET active_config_version = $3, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2`, record.TenantID, record.AppID, record.TargetConfigVersion); err != nil {
			return fmt.Errorf("activate migrated app config: %w", err)
		}
	}
	if record.Status == migration.StatusDraining && next == migration.StatusCopying {
		if err := ensureMigrationDrained(ctx, tx, record); err != nil {
			return err
		}
	}
	failureReason := ""
	if next == migration.StatusFailed {
		failureReason = platformlog.SafeError(errors.New(record.FailureReason))
	}
	tag, err := tx.Exec(ctx, `
UPDATE platform.data_migration
SET status = $6,
    failure_reason = CASE WHEN $6 = 'FAILED' THEN $8 ELSE failure_reason END,
    lease_owner = CASE WHEN $6 IN ('SUCCEEDED', 'FAILED') THEN NULL ELSE lease_owner END,
    lease_until = CASE WHEN $6 IN ('SUCCEEDED', 'FAILED') THEN NULL ELSE lease_until END,
    run_token = CASE WHEN $6 IN ('SUCCEEDED', 'FAILED') THEN NULL ELSE run_token END,
    updated_at = clock_timestamp()
WHERE migration_id = $1 AND tenant_id = $2 AND app_id = $3
  AND status = $4 AND lease_owner = $5 AND run_token = $7
  AND lease_until > clock_timestamp()`,
		record.ID, record.TenantID, record.AppID, record.Status, record.LeaseOwner, next, record.RunToken, failureReason,
	)
	if err != nil {
		return fmt.Errorf("advance data migration: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("advance data migration: %w", migration.ErrLeaseLost)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit data migration transition: %w", err)
	}
	return nil
}

// ListDataMigrationSessionKeys returns the platform-owned session inventory
// for a migration. It intentionally does not inspect backend-specific keys.
func (s *Store) ListDataMigrationSessionKeys(
	ctx context.Context,
	record migration.Record,
) ([]session.Key, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	appName, err := tenant.Scope{TenantID: record.TenantID, AppID: record.AppID}.Key("runner")
	if err != nil {
		return nil, fmt.Errorf("build data migration session app name: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
SELECT session_principal_id, session_id
FROM platform.session_lane
WHERE tenant_id = $1 AND app_id = $2
ORDER BY session_principal_id, session_id`, record.TenantID, record.AppID)
	if err != nil {
		return nil, fmt.Errorf("list data migration session keys: %w", err)
	}
	defer rows.Close()

	keys := make([]session.Key, 0)
	for rows.Next() {
		var userID, sessionID string
		if err := rows.Scan(&userID, &sessionID); err != nil {
			return nil, fmt.Errorf("scan data migration session key: %w", err)
		}
		keys = append(keys, session.Key{AppName: appName, UserID: userID, SessionID: sessionID})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate data migration session keys: %w", err)
	}
	return keys, nil
}

type dataMigrationScanner interface {
	Scan(...any) error
}

func scanDataMigrationRecord(scanner dataMigrationScanner) (migration.Record, error) {
	var record migration.Record
	if err := scanner.Scan(
		&record.ID,
		&record.TenantID,
		&record.AppID,
		&record.SourceConfigVersion,
		&record.TargetConfigVersion,
		&record.Status,
		&record.LeaseOwner,
		&record.LeaseUntil,
		&record.RunToken,
		&record.DrainDeadline,
		&record.FailureReason,
	); err != nil {
		return migration.Record{}, err
	}
	if err := record.Validate(); err != nil {
		return migration.Record{}, fmt.Errorf("read data migration: %w", err)
	}
	return record, nil
}

func migrationBlocksAdmission(ctx context.Context, tx pgx.Tx, tenantID, appID string) (bool, error) {
	var blocked bool
	err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1 FROM platform.data_migration
    WHERE tenant_id = $1 AND app_id = $2
      AND status IN ('DRAINING', 'COPYING', 'VERIFYING')
)`, tenantID, appID).Scan(&blocked)
	if err != nil {
		return false, fmt.Errorf("check data migration admission gate: %w", err)
	}
	return blocked, nil
}

func ensureMigrationDrained(ctx context.Context, tx pgx.Tx, record migration.Record) error {
	var deadlineReached bool
	var activeExecutions bool
	err := tx.QueryRow(ctx, `
SELECT drain_deadline <= clock_timestamp(),
       EXISTS (
           SELECT 1 FROM platform.execution
           WHERE tenant_id = data_migration.tenant_id
             AND app_id = data_migration.app_id
             AND config_version = data_migration.source_config_version
             AND status IN ('PENDING', 'RUNNING')
       )
FROM platform.data_migration AS data_migration
WHERE migration_id = $1 AND tenant_id = $2 AND app_id = $3
  AND status = 'DRAINING' AND lease_owner = $4 AND run_token = $5
  AND lease_until > clock_timestamp()
FOR UPDATE`, record.ID, record.TenantID, record.AppID, record.LeaseOwner, record.RunToken).Scan(
		&deadlineReached,
		&activeExecutions,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock draining data migration: %w", migration.ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock draining data migration: %w", err)
	}
	if activeExecutions {
		if deadlineReached {
			return migration.ErrDrainDeadlineExceeded
		}
		return migration.ErrDrainIncomplete
	}
	return nil
}

func sameMigrationBehavior(source, target tenant.AppConfig) bool {
	return source.TenantID == target.TenantID &&
		source.AppID == target.AppID &&
		reflect.DeepEqual(source.Model, target.Model) &&
		reflect.DeepEqual(source.Tools, target.Tools) &&
		reflect.DeepEqual(source.KnowledgeBaseIDs, target.KnowledgeBaseIDs) &&
		reflect.DeepEqual(source.SecretRefs, target.SecretRefs) &&
		reflect.DeepEqual(source.ChannelBinding, target.ChannelBinding)
}

// validateSupportedDataMigration rejects backend transitions that the worker
// cannot copy without silently leaving one authoritative backend behind.
func validateSupportedDataMigration(source, target tenant.BackendConfig) error {
	if !sameBackendRef(source.Memory, target.Memory) || !sameBackendRef(source.Artifact, target.Artifact) {
		return errors.New("data migration only supports session backend changes")
	}
	if source.Session.Kind != tenant.BackendRedis || source.Session.Provider != "redis" {
		return errors.New("data migration source session backend must use redis")
	}
	if target.Session.Kind != tenant.BackendSQL || target.Session.Provider != "postgres" {
		return errors.New("data migration target session backend must use postgres")
	}
	return nil
}
