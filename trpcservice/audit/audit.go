// Package audit defines the small, durable audit contract used by the
// execution path. It intentionally contains metadata only, never payloads.
package audit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	ExecutionStarted   = "execution_started"
	ExecutionCompleted = "execution_completed"
	ExecutionFailed    = "execution_failed"

	ToolAllowed        = "tool_allowed"
	ToolDenied         = "tool_denied"
	ToolReviewRequired = "tool_review_required"
	ToolCompleted      = "tool_completed"
	ToolFailed         = "tool_failed"

	IMAccessDenied = "im_access_denied"
	BudgetRejected = "budget_rejected"
)

// Sink is the best-effort audit persistence boundary. Implementations must
// enforce tenant and application scope in every write.
type Sink interface {
	Record(context.Context, Event) error
}

// Event is the complete audit record. It deliberately has no raw request,
// message, tool argument, provider target, secret, or artifact fields.
type Event struct {
	TenantID      string
	AppID         string
	Channel       string
	UserID        string
	SessionID     string
	AgentName     string
	ToolName      string
	Decision      string
	Latency       time.Duration
	ErrorType     string
	Cost          *float64
	InputTokens   int
	OutputTokens  int
	TotalTokens   int
	TraceID       string
	RequestID     string
	ConfigVersion string
	EventType     string
	CreatedAt     time.Time
}

// Validate checks metadata invariants before it reaches persistence.
func (e Event) Validate() error {
	for name, value := range map[string]string{
		"tenant_id":      e.TenantID,
		"app_id":         e.AppID,
		"event_type":     e.EventType,
		"decision":       e.Decision,
		"trace_id":       e.TraceID,
		"request_id":     e.RequestID,
		"config_version": e.ConfigVersion,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if e.Latency < 0 {
		return errors.New("latency must be non-negative")
	}
	if e.InputTokens < 0 || e.OutputTokens < 0 || e.TotalTokens < 0 {
		return errors.New("token usage must be non-negative")
	}
	if e.Cost != nil && (*e.Cost < 0 || math.IsNaN(*e.Cost) || math.IsInf(*e.Cost, 0)) {
		return errors.New("cost must be a finite non-negative value")
	}
	return nil
}
