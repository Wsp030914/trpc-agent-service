package worker

import (
	"context"
	"errors"
	"time"
)

const (
	// AuditEventExecutionStarted records that a real Runner invocation began.
	AuditEventExecutionStarted = "execution_started"
	// AuditEventExecutionCompleted records a completed Runner invocation.
	AuditEventExecutionCompleted = "execution_completed"
	// AuditEventExecutionFailed records an invocation that did not complete.
	AuditEventExecutionFailed = "execution_failed"
	// AuditEventToolDecision records the decision immediately before a tool run.
	AuditEventToolDecision = "tool_decision"
)

// AuditDecision is the allowlisted result of a tool permission check.
type AuditDecision string

const (
	// AuditDecisionAllow records a permitted tool call.
	AuditDecisionAllow AuditDecision = "allow"
	// AuditDecisionDeny records a denied tool call.
	AuditDecisionDeny AuditDecision = "deny"
	// AuditDecisionAsk records a tool call requiring human review.
	AuditDecisionAsk AuditDecision = "ask"
)

// AuditErrorType classifies a failed execution without retaining error text.
type AuditErrorType string

const (
	// AuditErrorRunnerIncomplete records a Runner that did not complete.
	AuditErrorRunnerIncomplete AuditErrorType = "runner_incomplete"
)

// AuditEvent contains only allowlisted execution metadata. It never contains
// messages, credential values, or raw tool arguments.
type AuditEvent struct {
	Type      string
	ToolName  string
	Decision  AuditDecision
	Latency   time.Duration
	ErrorType AuditErrorType
}

// Validate checks that an audit event can be written without accepting an
// arbitrary payload.
func (e AuditEvent) Validate() error {
	switch e.Type {
	case AuditEventExecutionStarted, AuditEventExecutionCompleted:
		if e.ToolName != "" || e.Decision != "" || e.ErrorType != "" {
			return errors.New("execution audit event contains tool or error fields")
		}
	case AuditEventExecutionFailed:
		if e.ToolName != "" || e.Decision != "" || e.ErrorType != AuditErrorRunnerIncomplete {
			return errors.New("failed execution audit event is invalid")
		}
	case AuditEventToolDecision:
		if e.ToolName == "" || !validAuditDecision(e.Decision) || e.ErrorType != "" {
			return errors.New("tool decision audit event is invalid")
		}
	default:
		return errors.New("audit event type is invalid")
	}
	if e.Latency < 0 {
		return errors.New("audit latency cannot be negative")
	}
	return nil
}

func validAuditDecision(decision AuditDecision) bool {
	return decision == AuditDecisionAllow ||
		decision == AuditDecisionDeny ||
		decision == AuditDecisionAsk
}

// AuditSink persists allowlisted audit events. A failure prevents the worker
// from continuing past the point that requires the audit record.
type AuditSink interface {
	RecordAudit(ctx context.Context, exec Execution, event AuditEvent) error
}
