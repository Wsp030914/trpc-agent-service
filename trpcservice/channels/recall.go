package channels

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
)

// RecallRequest is a verified provider recall event. Provider adapters supply
// normalized identifiers and a payload digest; trusted scope comes from the
// resolved Binding and is never read from provider payload tenant fields.
type RecallRequest struct {
	TenantID          string
	AppID             string
	BindingID         string
	Channel           Channel
	ExternalEventID   string
	ExternalMessageID string
	PayloadHash       []byte
}

// Validate checks the scope, identifiers, and digest required for durable
// recall admission.
func (r RecallRequest) Validate() error {
	if r.TenantID == "" || r.AppID == "" || r.BindingID == "" {
		return errors.New("recall scope is required")
	}
	if err := r.Channel.Validate(); err != nil {
		return err
	}
	if _, err := NormalizeExternalID(r.ExternalEventID); err != nil {
		return fmt.Errorf("recall external event id: %w", err)
	}
	if _, err := NormalizeExternalID(r.ExternalMessageID); err != nil {
		return fmt.Errorf("recall external message id: %w", err)
	}
	if len(r.PayloadHash) != sha256.Size {
		return errors.New("recall payload hash must be a sha256 digest")
	}
	return nil
}

// RecallResult describes the durable result of one recall event. A running
// execution needs a best-effort ManagedRunner cancellation after this method
// commits; the database state remains authoritative.
type RecallResult struct {
	RequestID       string
	ExecutionStatus string
	Replayed        bool
	CancelRequested bool
}

// RecallAdmitter durably applies a verified provider recall event.
type RecallAdmitter interface {
	AdmitRecall(context.Context, RecallRequest) (RecallResult, error)
}
