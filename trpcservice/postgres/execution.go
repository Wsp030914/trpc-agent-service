package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	maxExecutionAttempts = 3
	blockedDispatchDelay = time.Second
	defaultDispatchBatch = 32
	// consumedOutboxRetention bounds how long consumed dispatch records are
	// kept for observability before RecoverDispatches removes them.
	consumedOutboxRetention = 24 * time.Hour
)

// Claim changes one dispatched execution into a worker-owned run. It retains
// PostgreSQL session-lane order even when Redis delivers entries out of order.
func (s *Store) Claim(ctx context.Context, dispatch queue.Dispatch, request queue.ClaimRequest) (queue.Claim, bool, error) {
	if err := s.validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := dispatch.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := request.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return queue.Claim{}, false, fmt.Errorf("begin execution claim: %w", err)
	}
	defer func() { rollback(tx) }()
	stored, active, err := lockExecutionForClaim(ctx, tx, dispatch)
	if err != nil {
		return queue.Claim{}, false, err
	}
	if !active || stored.status == "SUCCEEDED" || stored.status == "FAILED" {
		if err := consumeDispatch(ctx, tx, dispatch); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit unavailable execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if stored.status == "RUNNING" && stored.leaseUntil.After(time.Now()) {
		if err := consumeDispatch(ctx, tx, dispatch); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit duplicate execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	if stored.nextAttemptAt.After(time.Now()) || stored.hasEarlier {
		if err := deferDispatch(ctx, tx, dispatch, stored.nextAttemptAt); err != nil {
			return queue.Claim{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return queue.Claim{}, false, fmt.Errorf("commit deferred execution: %w", err)
		}
		return queue.Claim{}, false, nil
	}
	var leaseUntil time.Time
	token := uuid.NewString()
	err = tx.QueryRow(ctx, `UPDATE platform.execution
SET status = 'RUNNING', attempt = attempt + 1, lease_owner = $4, run_token = $5,
    lease_until = clock_timestamp() + $6::interval, next_attempt_at = clock_timestamp(),
    started_at = COALESCE(started_at, clock_timestamp()), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
RETURNING lease_until, attempt`, stored.tenantID, stored.appID, stored.requestID, request.Owner, token, intervalLiteral(request.LeaseDuration)).Scan(&leaseUntil, &stored.attempt)
	if err != nil {
		return queue.Claim{}, false, fmt.Errorf("claim execution: %w", err)
	}
	if err := consumeDispatch(ctx, tx, dispatch); err != nil {
		return queue.Claim{}, false, err
	}
	job, err := stored.executionJob()
	if err != nil {
		return queue.Claim{}, false, err
	}
	claim := queue.Claim{Job: job, TurnSeq: stored.turnSeq, Attempt: stored.attempt, Lease: queue.Lease{Owner: request.Owner, Token: token, Until: leaseUntil.UTC()}}
	if err := claim.Validate(); err != nil {
		return queue.Claim{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return queue.Claim{}, false, fmt.Errorf("commit execution claim: %w", err)
	}
	return claim, true, nil
}

// Renew extends one active run token. A lost or expired claim cannot be renewed.
func (s *Store) Renew(ctx context.Context, claim queue.Claim, duration time.Duration) (queue.Lease, error) {
	if err := s.validate(); err != nil {
		return queue.Lease{}, err
	}
	if err := claim.Validate(); err != nil {
		return queue.Lease{}, err
	}
	if duration <= 0 {
		return queue.Lease{}, errors.New("lease duration must be positive")
	}
	var until time.Time
	err := s.pool.QueryRow(ctx, `UPDATE platform.execution SET lease_until = clock_timestamp() + $6::interval, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()
RETURNING lease_until`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token, intervalLiteral(duration)).Scan(&until)
	if errors.Is(err, pgx.ErrNoRows) {
		return queue.Lease{}, fmt.Errorf("renew execution: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return queue.Lease{}, fmt.Errorf("renew execution: %w", err)
	}
	return queue.Lease{Owner: claim.Lease.Owner, Token: claim.Lease.Token, Until: until.UTC()}, nil
}

// Complete records a terminal result only when the caller still owns its run token.
func (s *Store) Complete(ctx context.Context, claim queue.Claim, status queue.CompletionStatus) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	if err := status.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.execution SET status = $6, lease_owner = NULL, run_token = NULL,
lease_until = NULL, finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'
  AND lease_owner = $4 AND run_token = $5 AND lease_until > clock_timestamp()`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token, status)
	if err != nil {
		return fmt.Errorf("complete execution: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("complete execution: %w", queue.ErrLeaseLost)
	}
	return nil
}

// Retry returns a failed run to PENDING and creates its next dispatch in the
// same transaction. It terminally fails after a bounded retry count.
func (s *Store) Retry(ctx context.Context, claim queue.Claim, cause error) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin execution retry: %w", err)
	}
	defer func() { rollback(tx) }()
	var attempt int
	err = tx.QueryRow(ctx, `SELECT attempt FROM platform.execution WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3 AND status='RUNNING' AND lease_owner=$4 AND run_token=$5 AND lease_until > clock_timestamp() FOR UPDATE`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token).Scan(&attempt)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("retry execution: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("lock execution retry: %w", err)
	}
	if attempt >= maxExecutionAttempts {
		_, err = tx.Exec(ctx, `UPDATE platform.execution SET status='FAILED', last_error=$4, lease_owner=NULL, run_token=NULL, lease_until=NULL, finished_at=clock_timestamp(), updated_at=clock_timestamp() WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), truncateExecutionError(cause))
	} else {
		delay := time.Duration(attempt) * time.Second
		_, err = tx.Exec(ctx, `UPDATE platform.execution SET status='PENDING', last_error=$4, lease_owner=NULL, run_token=NULL, lease_until=NULL, next_attempt_at=clock_timestamp()+$5::interval, updated_at=clock_timestamp() WHERE tenant_id=$1 AND app_id=$2 AND request_id=$3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), truncateExecutionError(cause), intervalLiteral(delay))
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id, app_id, request_id, next_attempt_at) VALUES ($1,$2,$3,clock_timestamp()+$4::interval)`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), intervalLiteral(delay))
		}
	}
	if err != nil {
		return fmt.Errorf("retry execution: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution retry: %w", err)
	}
	return nil
}

// ClaimDispatches leases pending outbox rows for a relay process.
func (s *Store) ClaimDispatches(ctx context.Context, owner string, leaseDuration time.Duration, limit int) ([]queue.Dispatch, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("dispatcher owner is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("dispatch lease duration must be positive")
	}
	if limit <= 0 {
		limit = defaultDispatchBatch
	}
	rows, err := s.pool.Query(ctx, `WITH candidates AS (
 SELECT o.outbox_id FROM platform.dispatch_outbox o JOIN platform.execution e USING (tenant_id,app_id,request_id)
 WHERE o.status='PENDING' AND o.next_attempt_at <= clock_timestamp() AND e.status='PENDING'
 ORDER BY o.outbox_id FOR UPDATE SKIP LOCKED LIMIT $3
) UPDATE platform.dispatch_outbox o SET status='PUBLISHING', lease_owner=$1, lease_until=clock_timestamp()+$2::interval, attempt=attempt+1, updated_at=clock_timestamp()
FROM candidates c WHERE o.outbox_id=c.outbox_id RETURNING o.outbox_id,o.tenant_id,o.app_id,o.request_id`, owner, intervalLiteral(leaseDuration), limit)
	if err != nil {
		return nil, fmt.Errorf("claim dispatch outbox: %w", err)
	}
	defer rows.Close()
	var result []queue.Dispatch
	for rows.Next() {
		var d queue.Dispatch
		if err := rows.Scan(&d.OutboxID, &d.TenantID, &d.AppID, &d.RequestID); err != nil {
			return nil, fmt.Errorf("scan dispatch outbox: %w", err)
		}
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dispatch outbox: %w", err)
	}
	return result, nil
}

// CompleteDispatch marks a successfully published stream message as sent.
func (s *Store) CompleteDispatch(ctx context.Context, dispatch queue.Dispatch, owner string) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='SENT', lease_owner=NULL, lease_until=NULL, updated_at=clock_timestamp() WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4 AND status='PUBLISHING' AND lease_owner=$5 AND lease_until > clock_timestamp()`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID, owner)
	if err != nil {
		return fmt.Errorf("complete dispatch: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.dispatch_outbox
WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("complete dispatch: %w", queue.ErrLeaseLost)
		}
		return fmt.Errorf("read dispatch completion: %w", err)
	}
	if status != "CONSUMED" {
		return fmt.Errorf("complete dispatch: %w", queue.ErrLeaseLost)
	}
	return nil
}

// RetryDispatch releases a relay claim so another publisher can retry it.
func (s *Store) RetryDispatch(ctx context.Context, dispatch queue.Dispatch, owner string, cause error) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='PENDING', lease_owner=NULL, lease_until=NULL, last_error=$6, next_attempt_at=clock_timestamp()+interval '1 second', updated_at=clock_timestamp() WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4 AND status='PUBLISHING' AND lease_owner=$5`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID, owner, truncateExecutionError(cause))
	if err != nil {
		return fmt.Errorf("retry dispatch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("retry dispatch: %w", queue.ErrLeaseLost)
	}
	return nil
}

// RecoverDispatches makes expired execution and relay leases dispatchable again.
func (s *Store) RecoverDispatches(ctx context.Context, staleAfter time.Duration) error {
	if err := s.validate(); err != nil {
		return err
	}
	if staleAfter <= 0 {
		return errors.New("dispatch stale duration must be positive")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin dispatch recovery: %w", err)
	}
	defer func() { rollback(tx) }()
	if _, err = tx.Exec(ctx, `UPDATE platform.execution SET status='PENDING', lease_owner=NULL, run_token=NULL, lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp() WHERE status='RUNNING' AND lease_until <= clock_timestamp()`); err != nil {
		return fmt.Errorf("recover expired executions: %w", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='PENDING', lease_owner=NULL, lease_until=NULL, next_attempt_at=clock_timestamp(), updated_at=clock_timestamp() WHERE (status='PUBLISHING' AND lease_until <= clock_timestamp()) OR (status='SENT' AND updated_at <= clock_timestamp()-$1::interval)`, intervalLiteral(staleAfter)); err != nil {
		return fmt.Errorf("recover dispatch outbox: %w", err)
	}
	if _, err = tx.Exec(ctx, `DELETE FROM platform.dispatch_outbox WHERE status='CONSUMED' AND updated_at <= clock_timestamp() - $1::interval`, intervalLiteral(consumedOutboxRetention)); err != nil {
		return fmt.Errorf("clean consumed dispatch outbox: %w", err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id,app_id,request_id)
SELECT e.tenant_id,e.app_id,e.request_id
FROM platform.execution e
JOIN platform.tenant t ON t.tenant_id=e.tenant_id
JOIN platform.agent_app a ON a.tenant_id=e.tenant_id AND a.app_id=e.app_id
WHERE e.status='PENDING' AND t.status='ACTIVE' AND a.status='ACTIVE'
  AND NOT EXISTS (SELECT 1 FROM platform.dispatch_outbox o WHERE o.tenant_id=e.tenant_id AND o.app_id=e.app_id AND o.request_id=e.request_id AND o.status IN ('PENDING','PUBLISHING','SENT'))`); err != nil {
		return fmt.Errorf("fill missing dispatch outbox: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit dispatch recovery: %w", err)
	}
	return nil
}

type storedExecution struct {
	tenantID, appID, requestID, sessionPrincipalID, sessionID, userID string
	turnSeq                                                           int64
	configVersion                                                     string
	tenantSource                                                      gateway.TenantSource
	command                                                           []byte
	traceID, status                                                   string
	attempt                                                           int
	nextAttemptAt, leaseUntil                                         time.Time
	hasEarlier                                                        bool
}

func lockExecutionForClaim(ctx context.Context, tx pgx.Tx, d queue.Dispatch) (storedExecution, bool, error) {
	var v storedExecution
	err := tx.QueryRow(ctx, `SELECT e.tenant_id,e.app_id,e.request_id,e.session_principal_id,e.session_id,e.user_id,e.turn_seq,e.config_version,e.tenant_source,e.command,e.trace_id,e.status,e.attempt,e.next_attempt_at,COALESCE(e.lease_until,'epoch'::timestamptz),EXISTS(SELECT 1 FROM platform.execution x WHERE x.tenant_id=e.tenant_id AND x.app_id=e.app_id AND x.session_principal_id=e.session_principal_id AND x.session_id=e.session_id AND x.turn_seq<e.turn_seq AND x.status IN ('PENDING','RUNNING')) FROM platform.execution e JOIN platform.tenant t ON t.tenant_id=e.tenant_id JOIN platform.agent_app a ON a.tenant_id=e.tenant_id AND a.app_id=e.app_id WHERE e.tenant_id=$1 AND e.app_id=$2 AND e.request_id=$3 FOR UPDATE OF e`, d.TenantID, d.AppID, d.RequestID).Scan(&v.tenantID, &v.appID, &v.requestID, &v.sessionPrincipalID, &v.sessionID, &v.userID, &v.turnSeq, &v.configVersion, &v.tenantSource, &v.command, &v.traceID, &v.status, &v.attempt, &v.nextAttemptAt, &v.leaseUntil, &v.hasEarlier)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, fmt.Errorf("lock dispatched execution: %w", err)
	}
	var active bool
	err = tx.QueryRow(ctx, `SELECT t.status='ACTIVE' AND a.status='ACTIVE' FROM platform.tenant t JOIN platform.agent_app a ON a.tenant_id=t.tenant_id WHERE t.tenant_id=$1 AND a.app_id=$2`, d.TenantID, d.AppID).Scan(&active)
	if err != nil {
		return v, false, fmt.Errorf("check execution availability: %w", err)
	}
	return v, active, nil
}
func (v storedExecution) executionJob() (execution.Job, error) {
	var c admissionCommand
	if err := json.Unmarshal(v.command, &c); err != nil {
		return execution.Job{}, fmt.Errorf("unmarshal execution command: %w", err)
	}
	if c.TenantID != v.tenantID || c.AppID != v.appID || c.SessionPrincipalID != v.sessionPrincipalID || c.SessionID != v.sessionID || c.UserID != v.userID {
		return execution.Job{}, errors.New("execution command does not match stored scope")
	}
	return execution.NewJob(v.requestID, v.tenantSource, tenant.RuntimeContext{TenantID: v.tenantID, AppID: v.appID, ConfigVersion: v.configVersion, SessionPrincipalID: v.sessionPrincipalID, SessionID: v.sessionID, UserID: v.userID, TraceID: v.traceID}, gateway.Message{Text: c.Text, ArtifactRefs: c.ArtifactRefs})
}
func consumeDispatch(ctx context.Context, tx pgx.Tx, dispatch queue.Dispatch) error {
	if err := dispatch.Validate(); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE platform.dispatch_outbox SET status='CONSUMED',lease_owner=NULL,lease_until=NULL,updated_at=clock_timestamp()
WHERE outbox_id=$1 AND tenant_id=$2 AND app_id=$3 AND request_id=$4`, dispatch.OutboxID, dispatch.TenantID, dispatch.AppID, dispatch.RequestID)
	if err != nil {
		return fmt.Errorf("consume dispatch: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return errors.New("consume dispatch: outbox identity does not match dispatch")
	}
	return nil
}
func deferDispatch(ctx context.Context, tx pgx.Tx, d queue.Dispatch, next time.Time) error {
	if next.Before(time.Now()) {
		next = time.Now().Add(blockedDispatchDelay)
	}
	if err := consumeDispatch(ctx, tx, d); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO platform.dispatch_outbox (tenant_id,app_id,request_id,next_attempt_at) VALUES ($1,$2,$3,$4)`, d.TenantID, d.AppID, d.RequestID, next)
	if err != nil {
		return fmt.Errorf("defer dispatch: %w", err)
	}
	return nil
}
func intervalLiteral(v time.Duration) string { return fmt.Sprintf("%d microseconds", v.Microseconds()) }
func truncateExecutionError(err error) string {
	return platformlog.SafeError(err)
}
