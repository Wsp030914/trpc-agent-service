// Package migration defines platform-owned data migration coordination values.
package migration

import (
	"errors"
	"time"
)

var (
	// ErrDrainDeadlineExceeded means accepted source executions did not finish
	// before the migration maintenance window ended.
	ErrDrainDeadlineExceeded = errors.New("data migration drain deadline exceeded")
	// ErrDrainIncomplete means accepted source executions are still draining.
	// The current owner should retry while its lease remains valid.
	ErrDrainIncomplete = errors.New("data migration source executions have not drained")
	// ErrLeaseLost means a different worker owns the migration or its lease
	// expired. The caller must stop without changing the durable record.
	ErrLeaseLost = errors.New("data migration lease lost")
)

// RetryableError tells the migration executor that an operation failed because
// its backend is temporarily unavailable and the durable phase must remain
// resumable. The copier must make each phase idempotent when returning one.
type RetryableError struct{ Err error }

func (e RetryableError) Error() string {
	if e.Err == nil {
		return "retryable data migration error"
	}
	return e.Err.Error()
}

func (e RetryableError) Unwrap() error     { return e.Err }
func (e RetryableError) IsRetryable() bool { return true }

func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return RetryableError{Err: err}
}

// Status identifies the durable lifecycle of one backend data migration.
type Status string

const (
	// StatusPending means validation completed but request admission remains open.
	StatusPending Status = "PENDING"
	// StatusDraining rejects new admissions while accepted executions complete.
	StatusDraining Status = "DRAINING"
	// StatusCopying moves authoritative data from the source to the target.
	StatusCopying Status = "COPYING"
	// StatusVerifying validates copied authoritative data before activation.
	StatusVerifying Status = "VERIFYING"
	// StatusSucceeded means the target config was activated atomically.
	StatusSucceeded Status = "SUCCEEDED"
	// StatusFailed means the source config remains active after migration failure.
	StatusFailed Status = "FAILED"
)

// Record identifies one Tenant/App migration between immutable config versions.
type Record struct {
	ID                  string    `json:"migration_id"`
	TenantID            string    `json:"tenant_id"`
	AppID               string    `json:"app_id"`
	SourceConfigVersion string    `json:"source_config_version"`
	TargetConfigVersion string    `json:"target_config_version"`
	Status              Status    `json:"status"`
	LeaseOwner          string    `json:"lease_owner,omitempty"`
	LeaseUntil          time.Time `json:"lease_until,omitempty"`
	RunToken            string    `json:"run_token,omitempty"`
	DrainDeadline       time.Time `json:"drain_deadline,omitempty"`
	FailureReason       string    `json:"failure_reason,omitempty"`
}

// Validate checks the persisted identity and lifecycle fields of Record.
func (r Record) Validate() error {
	if r.ID == "" || r.TenantID == "" || r.AppID == "" {
		return errors.New("data migration identity is required")
	}
	if r.SourceConfigVersion == "" || r.TargetConfigVersion == "" {
		return errors.New("data migration config versions are required")
	}
	if r.SourceConfigVersion == r.TargetConfigVersion {
		return errors.New("data migration target config must differ from source")
	}
	if !validStatus(r.Status) {
		return errors.New("data migration status is invalid")
	}
	if r.LeaseOwner == "" && (!r.LeaseUntil.IsZero() || r.RunToken != "") {
		return errors.New("data migration lease owner is required")
	}
	if r.LeaseOwner != "" && (r.LeaseUntil.IsZero() || r.RunToken == "") {
		return errors.New("data migration lease is incomplete")
	}
	return nil
}

// IsTerminal reports whether Record no longer blocks new admissions.
func (r Record) IsTerminal() bool {
	return r.Status == StatusSucceeded || r.Status == StatusFailed
}

// CanTransition reports whether the durable state machine allows next.
func (r Record) CanTransition(next Status) bool {
	switch r.Status {
	case StatusPending:
		return next == StatusDraining || next == StatusFailed
	case StatusDraining:
		return next == StatusCopying || next == StatusFailed
	case StatusCopying:
		return next == StatusVerifying || next == StatusFailed
	case StatusVerifying:
		return next == StatusSucceeded || next == StatusFailed
	default:
		return false
	}
}

func validStatus(status Status) bool {
	switch status {
	case StatusPending, StatusDraining, StatusCopying, StatusVerifying, StatusSucceeded, StatusFailed:
		return true
	default:
		return false
	}
}
