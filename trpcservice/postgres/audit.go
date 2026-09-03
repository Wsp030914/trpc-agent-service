package postgres

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

var _ platformaudit.Sink = (*Store)(nil)

// Record persists metadata-only audit events in the exact tenant/app scope.
// Callers use this as a best-effort sink; an audit failure must not be used to
// roll back execution or reply state.
func (s *Store) Record(ctx context.Context, event platformaudit.Event) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		return err
	}
	createdAt := event.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	if _, err := s.pool.Exec(ctx, `
INSERT INTO platform.audit_event (
    tenant_id, app_id, channel, user_id, session_id, agent_name, tool_name,
    decision, latency, error_type, input_tokens, output_tokens, total_tokens,
    cost, trace_id, request_id, config_version, event_type, created_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)`,
		event.TenantID,
		event.AppID,
		event.Channel,
		event.UserID,
		event.SessionID,
		event.AgentName,
		event.ToolName,
		event.Decision,
		event.Latency.Milliseconds(),
		event.ErrorType,
		event.InputTokens,
		event.OutputTokens,
		event.TotalTokens,
		event.Cost,
		event.TraceID,
		event.RequestID,
		event.ConfigVersion,
		event.EventType,
		createdAt,
	); err != nil {
		return fmt.Errorf("insert audit event: %w", err)
	}
	return nil
}

func (s *Store) recordAuditBestEffort(ctx context.Context, event platformaudit.Event) {
	if s == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := s.Record(auditCtx, event); err != nil {
		log.Printf("audit write failed tenant=%s app=%s event=%s: %s", event.TenantID, event.AppID, event.EventType, platformlog.SafeError(err))
		if s.metrics != nil {
			s.metrics.RecordAuditFailure(ctx, platformmetrics.Labels{
				TenantID: event.TenantID,
				AppID:    event.AppID,
				Channel:  event.Channel,
			})
		}
	}
}

// ListAuditEvents reads only the requested tenant/application partition.
func (s *Store) ListAuditEvents(ctx context.Context, tenantID, appID string, limit int) ([]platformaudit.Event, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if tenantID == "" || appID == "" {
		return nil, errors.New("tenant_id and app_id are required")
	}
	if limit <= 0 || limit > 1000 {
		limit = 1000
	}
	rows, err := s.pool.Query(ctx, `
SELECT tenant_id, app_id, channel, user_id, session_id, agent_name, tool_name,
       decision, latency, error_type, input_tokens, output_tokens, total_tokens,
       cost, trace_id, request_id, config_version, event_type, created_at
FROM platform.audit_event
WHERE tenant_id = $1 AND app_id = $2
ORDER BY created_at DESC, audit_event_id DESC
LIMIT $3`, tenantID, appID, limit)
	if err != nil {
		return nil, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()
	result := make([]platformaudit.Event, 0)
	for rows.Next() {
		var event platformaudit.Event
		var latency int64
		if err := rows.Scan(
			&event.TenantID,
			&event.AppID,
			&event.Channel,
			&event.UserID,
			&event.SessionID,
			&event.AgentName,
			&event.ToolName,
			&event.Decision,
			&latency,
			&event.ErrorType,
			&event.InputTokens,
			&event.OutputTokens,
			&event.TotalTokens,
			&event.Cost,
			&event.TraceID,
			&event.RequestID,
			&event.ConfigVersion,
			&event.EventType,
			&event.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan audit event: %w", err)
		}
		event.Latency = time.Duration(latency) * time.Millisecond
		result = append(result, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate audit events: %w", err)
	}
	return result, nil
}
