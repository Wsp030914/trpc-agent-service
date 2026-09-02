package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

const executionEventPollInterval = 200 * time.Millisecond

// ExecutionEventJournal persists runner events and exposes them as a durable,
// tenant-scoped stream for protocol adapters.
type ExecutionEventJournal struct {
	store        *Store
	replyBuilder *worker.ReplyEventBuilder
}

// ExecutionEventJournalOption configures optional durable event projections.
type ExecutionEventJournalOption func(*ExecutionEventJournal) error

// WithReplyEventBuilder enables the IM Reply Projection inside the same
// PostgreSQL transaction as execution_event insertion.
func WithReplyEventBuilder(builder *worker.ReplyEventBuilder) ExecutionEventJournalOption {
	return func(journal *ExecutionEventJournal) error {
		if builder == nil {
			return errors.New("reply event builder is required")
		}
		journal.replyBuilder = builder
		return nil
	}
}

// NewExecutionEventJournal creates a journal backed by Store's PostgreSQL
// database. The store remains owned by the caller.
func NewExecutionEventJournal(store *Store, options ...ExecutionEventJournalOption) (*ExecutionEventJournal, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	journal := &ExecutionEventJournal{store: store}
	for _, option := range options {
		if option == nil {
			return nil, errors.New("execution event journal option is required")
		}
		if err := option(journal); err != nil {
			return nil, fmt.Errorf("configure execution event journal: %w", err)
		}
	}
	return journal, nil
}

// HandleRunnerEvent appends one real Runner event. It serializes concurrent
// appends and revalidates the active execution run token in the same
// transaction.
func (j *ExecutionEventJournal) HandleRunnerEvent(
	ctx context.Context,
	exec worker.Execution,
	evt *event.Event,
) error {
	if j == nil || j.store == nil {
		return errors.New("execution event journal is not initialized")
	}
	if err := j.store.validate(); err != nil {
		return err
	}
	if evt == nil {
		return errors.New("runner event is required")
	}
	if exec.RequestID == "" {
		return errors.New("execution request_id is required")
	}
	if err := exec.Tenant.Validate(); err != nil {
		return fmt.Errorf("execution tenant context: %w", err)
	}
	lease, ok := worker.JobLeaseFromContext(ctx)
	if !ok {
		return errors.New("execution event requires a current execution lease")
	}
	payload, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal execution event: %w", err)
	}

	tx, err := j.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin execution event append: %w", err)
	}
	defer func() {
		rollback(tx)
	}()
	if err := tx.QueryRow(
		ctx,
		`SELECT 1
FROM platform.execution
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND status = 'RUNNING'
  AND lease_owner = $4
  AND run_token = $5
  AND lease_until > clock_timestamp()
  AND session_principal_id = $6
  AND session_id = $7
  AND user_id = $8
  AND config_version = $9
FOR UPDATE`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		lease.Owner,
		lease.Token,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		exec.Tenant.UserID,
		exec.Tenant.ConfigVersion,
	).Scan(new(int)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock execution for event append: %w", queue.ErrLeaseLost)
		}
		return fmt.Errorf("lock execution for event append: %w", err)
	}
	var sequence int64
	if err := tx.QueryRow(
		ctx,
		`SELECT COALESCE(MAX(event_seq), 0) + 1
FROM platform.execution_event
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
	).Scan(&sequence); err != nil {
		return fmt.Errorf("next execution event sequence: %w", err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.execution_event (
    tenant_id, app_id, request_id, event_seq, event_type, payload
) VALUES ($1, $2, $3, $4, $5, $6)`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		sequence,
		executionEventType(evt),
		payload,
	); err != nil {
		return fmt.Errorf("insert execution event: %w", err)
	}
	if j.replyBuilder != nil {
		replies, err := j.replyBuilder.Build(ctx, exec, sequence, evt)
		if err != nil {
			return fmt.Errorf("build reply projection: %w", err)
		}
		if err := applyReplyProjectionTx(
			ctx,
			tx,
			exec.Tenant.TenantID,
			exec.Tenant.AppID,
			exec.Tenant.BindingID,
			exec.RequestID,
			sequence,
			replies,
		); err != nil {
			return fmt.Errorf("apply reply projection: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit execution event append: %w", err)
	}
	return nil
}

// SubscribeExecutionEvents streams persisted events after afterSequence. The
// stream closes after a Runner completion or terminal-error event, or when the
// caller cancels ctx.
func (j *ExecutionEventJournal) SubscribeExecutionEvents(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) (<-chan gateway.ExecutionEvent, error) {
	if j == nil || j.store == nil {
		return nil, errors.New("execution event journal is not initialized")
	}
	if err := j.store.validate(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if requestID == "" {
		return nil, errors.New("request_id is required")
	}
	if afterSequence < 0 {
		return nil, errors.New("execution event sequence cannot be negative")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	exists, err := j.executionExists(ctx, scope, requestID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("execution: %w", ErrNotFound)
	}
	events := make(chan gateway.ExecutionEvent)
	go j.streamExecutionEvents(ctx, events, scope, requestID, afterSequence)
	return events, nil
}

func (j *ExecutionEventJournal) streamExecutionEvents(
	ctx context.Context,
	output chan<- gateway.ExecutionEvent,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) {
	defer close(output)
	for {
		items, err := j.executionEventsAfter(ctx, scope, requestID, afterSequence)
		if err != nil {
			return
		}
		for _, item := range items {
			select {
			case <-ctx.Done():
				return
			case output <- item:
			}
			afterSequence = item.Sequence
			if item.Event.IsRunnerCompletion() || item.Event.IsTerminalError() {
				return
			}
		}
		status, err := j.executionStatus(ctx, scope, requestID)
		if err != nil {
			return
		}
		switch status {
		case "FAILED", "CANCELED":
			terminal := event.NewErrorEvent(
				requestID,
				"platform",
				"execution_"+strings.ToLower(status),
				"execution did not complete",
			)
			select {
			case <-ctx.Done():
				return
			case output <- gateway.ExecutionEvent{Sequence: afterSequence + 1, Event: terminal}:
			}
			return
		case "SUCCEEDED":
			return
		}
		timer := time.NewTimer(executionEventPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func (j *ExecutionEventJournal) executionExists(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
) (bool, error) {
	var exists bool
	if err := j.store.pool.QueryRow(
		ctx,
		`SELECT EXISTS(
    SELECT 1
    FROM platform.execution
    WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3
)`,
		scope.TenantID,
		scope.AppID,
		requestID,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check execution: %w", err)
	}
	return exists, nil
}

func (j *ExecutionEventJournal) executionStatus(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
) (string, error) {
	var status string
	if err := j.store.pool.QueryRow(
		ctx,
		`SELECT status
FROM platform.execution
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3`,
		scope.TenantID,
		scope.AppID,
		requestID,
	).Scan(&status); err != nil {
		return "", fmt.Errorf("read execution status: %w", resolveError("execution", err))
	}
	return status, nil
}

func (j *ExecutionEventJournal) executionEventsAfter(
	ctx context.Context,
	scope tenant.Scope,
	requestID string,
	afterSequence int64,
) ([]gateway.ExecutionEvent, error) {
	rows, err := j.store.pool.Query(
		ctx,
		`SELECT event_seq, payload
FROM platform.execution_event
WHERE tenant_id = $1 AND app_id = $2 AND request_id = $3 AND event_seq > $4
ORDER BY event_seq`,
		scope.TenantID,
		scope.AppID,
		requestID,
		afterSequence,
	)
	if err != nil {
		return nil, fmt.Errorf("query execution events: %w", err)
	}
	defer rows.Close()
	var events []gateway.ExecutionEvent
	for rows.Next() {
		var item gateway.ExecutionEvent
		var payload []byte
		if err := rows.Scan(&item.Sequence, &payload); err != nil {
			return nil, fmt.Errorf("scan execution event: %w", err)
		}
		item.Event = &event.Event{}
		if err := json.Unmarshal(payload, item.Event); err != nil {
			return nil, fmt.Errorf("unmarshal execution event: %w", err)
		}
		if err := item.Validate(); err != nil {
			return nil, fmt.Errorf("stored execution event: %w", err)
		}
		events = append(events, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate execution events: %w", err)
	}
	return events, nil
}

func executionEventType(evt *event.Event) string {
	if evt.Response != nil && evt.Object != "" {
		return evt.Object
	}
	return "runner_event"
}

var _ gateway.ExecutionEventSource = (*ExecutionEventJournal)(nil)
var _ worker.EventSink = (*ExecutionEventJournal)(nil)

// AuditStore writes append-only, allowlisted audit events to platform SQL.
type AuditStore struct {
	store *Store
}

// NewAuditStore creates an audit writer backed by Store's PostgreSQL database.
// The store remains owned by the caller.
func NewAuditStore(store *Store) (*AuditStore, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	if err := store.validate(); err != nil {
		return nil, err
	}
	return &AuditStore{store: store}, nil
}

// RecordAudit writes one audit event only while the caller owns the current
// job lease. It stores no runner event payload, message body, or credentials.
func (s *AuditStore) RecordAudit(
	ctx context.Context,
	exec worker.Execution,
	auditEvent worker.AuditEvent,
) error {
	if s == nil || s.store == nil {
		return errors.New("audit store is not initialized")
	}
	if err := s.store.validate(); err != nil {
		return err
	}
	if err := auditEvent.Validate(); err != nil {
		return err
	}
	lease, ok := worker.JobLeaseFromContext(ctx)
	if !ok {
		return errors.New("audit event requires a current execution lease")
	}
	if err := exec.Tenant.Validate(); err != nil {
		return fmt.Errorf("execution tenant context: %w", err)
	}
	if exec.RequestID == "" {
		return errors.New("execution request_id is required")
	}

	tx, err := s.store.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin audit append: %w", err)
	}
	defer func() { rollback(tx) }()
	if err := tx.QueryRow(ctx, `
SELECT 1
FROM platform.execution
WHERE tenant_id = $1
  AND app_id = $2
  AND request_id = $3
  AND status = 'RUNNING'
  AND lease_owner = $4
  AND run_token = $5
  AND lease_until > clock_timestamp()
  AND session_principal_id = $6
  AND session_id = $7
  AND user_id = $8
  AND config_version = $9
FOR UPDATE`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		lease.Owner,
		lease.Token,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		exec.Tenant.UserID,
		exec.Tenant.ConfigVersion,
	).Scan(new(int)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("lock execution for audit append: %w", queue.ErrLeaseLost)
		}
		return fmt.Errorf("lock execution for audit append: %w", err)
	}
	var expireAt *time.Time
	if exec.Config.Audit.RetentionDays > 0 {
		value := time.Now().UTC().AddDate(0, 0, exec.Config.Audit.RetentionDays)
		expireAt = &value
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.audit_event (
    tenant_id, app_id, request_id, channel, user_id, session_principal_id, session_id, trace_id,
    agent_name, event_type, tool_name, decision, latency_ms, error_type, expire_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
)`,
		exec.Tenant.TenantID,
		exec.Tenant.AppID,
		exec.RequestID,
		exec.Tenant.Channel,
		exec.Tenant.UserID,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		exec.Tenant.TraceID,
		"assistant",
		auditEvent.Type,
		auditEvent.ToolName,
		string(auditEvent.Decision),
		auditEvent.Latency.Milliseconds(),
		string(auditEvent.ErrorType),
		expireAt,
	); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit audit append: %w", err)
	}
	return nil
}

var _ worker.AuditSink = (*AuditStore)(nil)
