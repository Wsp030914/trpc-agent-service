package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

// RecallRequest is retained as a package alias for callers that used the
// PostgreSQL implementation before the channel boundary was introduced.
type RecallRequest = channels.RecallRequest

// RecallResult is retained as a package alias for callers of HandleRecall.
type RecallResult = channels.RecallResult

// HandleRecall idempotently records a verified recall and applies the smallest
// state change allowed by the execution status. It never deletes session data,
// tool results, inbox rows, or replies.
func (s *Store) HandleRecall(ctx context.Context, request RecallRequest) (RecallResult, error) {
	if err := s.validate(); err != nil {
		return RecallResult{}, err
	}
	if err := request.Validate(); err != nil {
		return RecallResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return RecallResult{}, fmt.Errorf("begin recall admission: %w", err)
	}
	defer func() { rollback(tx) }()
	var existingHash []byte
	var existingRequest *string
	var existingStatus string
	err = tx.QueryRow(ctx, `
SELECT payload_hash, request_id, status
FROM platform.channel_recall_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND external_event_id = $4
FOR UPDATE`, request.TenantID, request.AppID, request.BindingID, request.ExternalEventID).Scan(&existingHash, &existingRequest, &existingStatus)
	if err == nil {
		if !bytes.Equal(existingHash, request.PayloadHash) {
			return RecallResult{}, gateway.ErrIdempotencyConflict
		}
		result := RecallResult{Replayed: true}
		if existingRequest != nil {
			result.RequestID = *existingRequest
		}
		result.ExecutionStatus = existingStatus
		if err := tx.Commit(ctx); err != nil {
			return RecallResult{}, fmt.Errorf("commit replayed recall: %w", err)
		}
		return result, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return RecallResult{}, fmt.Errorf("find recall event: %w", err)
	}
	var inboxRequestID string
	err = tx.QueryRow(ctx, `
SELECT request_id
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND external_message_id = $4
FOR UPDATE`, request.TenantID, request.AppID, request.BindingID, request.ExternalMessageID).Scan(&inboxRequestID)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_recall_inbox (
    tenant_id, app_id, binding_id, external_event_id, request_id,
    payload_hash, status, reject_reason
) VALUES ($1, $2, $3, $4, NULL, $5, 'REJECTED', 'REQUEST_NOT_FOUND')`,
			request.TenantID, request.AppID, request.BindingID, request.ExternalEventID, request.PayloadHash); err != nil {
			return RecallResult{}, fmt.Errorf("insert rejected recall: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return RecallResult{}, fmt.Errorf("commit rejected recall: %w", err)
		}
		return RecallResult{ExecutionStatus: "REJECTED"}, nil
	}
	if err != nil {
		return RecallResult{}, fmt.Errorf("find recalled inbox: %w", err)
	}
	var executionStatus, channel, userID, principalID, sessionID, traceID string
	err = tx.QueryRow(ctx, `
SELECT status, channel, user_id, session_principal_id, session_id, trace_id
FROM platform.execution e
JOIN platform.channel_binding b
  ON b.tenant_id = e.tenant_id AND b.app_id = e.app_id AND b.binding_id = $3
WHERE e.tenant_id = $1 AND e.app_id = $2 AND e.request_id = $4
  AND e.command->>'binding_id' = $3
FOR UPDATE`, request.TenantID, request.AppID, request.BindingID, inboxRequestID).Scan(
		&executionStatus, &channel, &userID, &principalID, &sessionID, &traceID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_recall_inbox (
    tenant_id, app_id, binding_id, external_event_id, request_id,
    payload_hash, status, reject_reason
) VALUES ($1, $2, $3, $4, $5, $6, 'REJECTED', 'REQUEST_NOT_FOUND')`,
			request.TenantID, request.AppID, request.BindingID, request.ExternalEventID, inboxRequestID, request.PayloadHash); err != nil {
			return RecallResult{}, fmt.Errorf("insert unlinked recall: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return RecallResult{}, fmt.Errorf("commit unlinked recall: %w", err)
		}
		return RecallResult{RequestID: inboxRequestID, ExecutionStatus: "REJECTED"}, nil
	}
	if err != nil {
		return RecallResult{}, fmt.Errorf("lock recalled execution: %w", err)
	}
	if channels.Channel(channel) != request.Channel {
		return RecallResult{}, channels.ErrBindingChannelMismatch
	}
	result := RecallResult{RequestID: inboxRequestID, ExecutionStatus: executionStatus}
	switch executionStatus {
	case "PENDING":
		if _, err := tx.Exec(ctx, `
UPDATE platform.execution
SET status = 'CANCELED', cancel_requested = true, cancel_requested_at = clock_timestamp(),
    finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'PENDING'`,
			request.TenantID, request.AppID, inboxRequestID); err != nil {
			return RecallResult{}, fmt.Errorf("cancel pending execution: %w", err)
		}
		result.ExecutionStatus = "CANCELED"
	case "RUNNING":
		if _, err := tx.Exec(ctx, `
UPDATE platform.execution
SET cancel_requested = true, cancel_requested_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND status = 'RUNNING'`,
			request.TenantID, request.AppID, inboxRequestID); err != nil {
			return RecallResult{}, fmt.Errorf("request running execution cancellation: %w", err)
		}
		result.CancelRequested = true
	default:
		// Completed and previously canceled executions are retained. The audit
		// row below records that the recall was verified and observed.
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.channel_recall_inbox (
    tenant_id, app_id, binding_id, external_event_id, request_id,
    payload_hash, status
) VALUES ($1, $2, $3, $4, $5, $6, 'APPLIED')`,
		request.TenantID, request.AppID, request.BindingID, request.ExternalEventID, inboxRequestID, request.PayloadHash); err != nil {
		return RecallResult{}, fmt.Errorf("insert applied recall: %w", err)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.audit_event (
    tenant_id, app_id, request_id, channel, user_id, session_principal_id,
    session_id, trace_id, agent_name, event_type, decision
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'channel', 'CHANNEL_MESSAGE_RECALLED', 'ALLOW')`,
		request.TenantID, request.AppID, inboxRequestID, channel, userID, principalID, sessionID, traceID); err != nil {
		return RecallResult{}, fmt.Errorf("record recall audit: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RecallResult{}, fmt.Errorf("commit recall admission: %w", err)
	}
	return result, nil
}

// AdmitRecall implements the provider-neutral channel recall boundary.
func (s *Store) AdmitRecall(ctx context.Context, request channels.RecallRequest) (channels.RecallResult, error) {
	return s.HandleRecall(ctx, request)
}

// Cancel marks a currently claimed execution canceled after its ManagedRunner
// has been asked to stop. It is a narrow execution-store hook for recall.
func (s *Store) Cancel(ctx context.Context, claim queue.Claim) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := claim.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.execution
SET status = 'CANCELED', cancel_requested = true, cancel_requested_at = COALESCE(cancel_requested_at, clock_timestamp()),
    lease_owner = NULL, run_token = NULL, lease_until = NULL,
    finished_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
  AND status = 'RUNNING' AND lease_owner = $4 AND run_token = $5`,
		claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID(), claim.Lease.Owner, claim.Lease.Token)
	if err != nil {
		return fmt.Errorf("cancel execution: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	var cancelRequested bool
	if err := s.pool.QueryRow(ctx, `SELECT status, cancel_requested FROM platform.execution WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`, claim.Job.Tenant().TenantID, claim.Job.Tenant().AppID, claim.Job.RequestID()).Scan(&status, &cancelRequested); err != nil {
		return fmt.Errorf("read canceled execution: %w", err)
	}
	if status == "CANCELED" && cancelRequested {
		return nil
	}
	return fmt.Errorf("cancel execution: %w", queue.ErrLeaseLost)
}

// CancellationRequested reports whether a recall has requested cancellation
// for the currently owned execution. A missing or expired lease is not a
// negative result; it is a lost claim and must be handled as such.
func (s *Store) CancellationRequested(ctx context.Context, claim queue.Claim) (bool, error) {
	if err := s.validate(); err != nil {
		return false, err
	}
	if err := claim.Validate(); err != nil {
		return false, err
	}
	var requested bool
	err := s.pool.QueryRow(ctx, `
SELECT cancel_requested
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
  AND status = 'RUNNING' AND lease_owner = $4 AND run_token = $5
  AND lease_until > clock_timestamp()`,
		claim.Job.Tenant().TenantID,
		claim.Job.Tenant().AppID,
		claim.Job.RequestID(),
		claim.Lease.Owner,
		claim.Lease.Token,
	).Scan(&requested)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("check execution cancellation: %w", queue.ErrLeaseLost)
	}
	if err != nil {
		return false, fmt.Errorf("check execution cancellation: %w", err)
	}
	return requested, nil
}

var _ interface {
	Cancel(context.Context, queue.Claim) error
} = (*Store)(nil)
var _ interface {
	CancellationRequested(context.Context, queue.Claim) (bool, error)
} = (*Store)(nil)
var _ channels.RecallAdmitter = (*Store)(nil)
