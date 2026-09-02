package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

var (
	// ErrReplyProjectionGap means a later execution event arrived before its
	// predecessor and must be retried after the predecessor is projected.
	ErrReplyProjectionGap = errors.New("reply projection event gap")
	// ErrReplyLeaseLost means a sender no longer owns the Reply Outbox row.
	ErrReplyLeaseLost = errors.New("reply outbox lease is lost")
	// ErrReplyTargetExpired means a message-scoped provider target cannot be
	// replaced with a stable user or conversation target.
	ErrReplyTargetExpired = errors.New("reply target is expired")
)

type replyProjectionState struct {
	lastEventSeq int64
	content      string
}

type storedReplyPayload struct {
	Text        string          `json:"text,omitempty"`
	Card        json.RawMessage `json:"card,omitempty"`
	ArtifactRef string          `json:"artifact_ref,omitempty"`
}

type storedReplyTarget struct {
	Kind             channels.TargetKind `json:"target_kind"`
	InternalEntityID string              `json:"internal_entity_id"`
}

// PutReply persists one Reply and advances its event projection cursor. It is
// a convenience boundary for tests and small integrations; the execution
// event journal uses the same operation inside its event append transaction.
func (s *Store) PutReply(ctx context.Context, reply channels.Reply) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := reply.Validate(); err != nil {
		return err
	}
	sequence, err := replyEventSequence(reply.SourceEventID, reply.RequestID)
	if err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin reply projection: %w", err)
	}
	defer func() { rollback(tx) }()
	if err := applyReplyProjectionTx(ctx, tx, reply.TenantID, reply.AppID, reply.BindingID, reply.RequestID, sequence, []channels.Reply{reply}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reply projection: %w", err)
	}
	return nil
}

// AdvanceReplyProjection moves a projection cursor for a non-user-visible
// execution event without creating an outbound reply.
func (s *Store) AdvanceReplyProjection(
	ctx context.Context,
	tenantID, appID, bindingID, requestID string,
	sequence int64,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if tenantID == "" || appID == "" || bindingID == "" || requestID == "" || sequence <= 0 {
		return errors.New("reply projection scope and sequence are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin reply projection advance: %w", err)
	}
	defer func() { rollback(tx) }()
	if err := applyReplyProjectionTx(ctx, tx, tenantID, appID, bindingID, requestID, sequence, nil); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit reply projection advance: %w", err)
	}
	return nil
}

func applyReplyProjectionTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, requestID string,
	sequence int64,
	replies []channels.Reply,
) error {
	if sequence <= 0 {
		return errors.New("reply projection sequence must be positive")
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO platform.reply_projection_state (
    tenant_id, app_id, binding_id, request_id, last_event_seq
) VALUES ($1, $2, $3, $4, 0)
ON CONFLICT (tenant_id, app_id, binding_id, request_id) DO NOTHING`, tenantID, appID, bindingID, requestID); err != nil {
		return fmt.Errorf("create reply projection state: %w", err)
	}
	var state replyProjectionState
	if err := tx.QueryRow(ctx, `
SELECT last_event_seq, content
FROM platform.reply_projection_state
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND request_id = $4
FOR UPDATE`, tenantID, appID, bindingID, requestID).Scan(&state.lastEventSeq, &state.content); err != nil {
		return fmt.Errorf("lock reply projection state: %w", err)
	}
	if sequence <= state.lastEventSeq {
		return nil
	}
	if sequence != state.lastEventSeq+1 {
		return fmt.Errorf("%w: expected %d, got %d", ErrReplyProjectionGap, state.lastEventSeq+1, sequence)
	}
	for _, reply := range replies {
		if reply.TenantID != tenantID || reply.AppID != appID || reply.BindingID != bindingID || reply.RequestID != requestID {
			return errors.New("reply projection scope does not match state")
		}
		effective, err := normalizeProjectedReply(ctx, tx, reply, state.content)
		if err != nil {
			return err
		}
		if effective == nil {
			continue
		}
		payload, err := json.Marshal(storedReplyPayload{
			Text:        effective.Text,
			Card:        effective.Card,
			ArtifactRef: effective.ArtifactRef,
		})
		if err != nil {
			return fmt.Errorf("marshal reply payload: %w", err)
		}
		target, err := json.Marshal(storedReplyTarget{
			Kind:             effective.Target.Kind,
			InternalEntityID: effective.Target.InternalEntityID,
		})
		if err != nil {
			return fmt.Errorf("marshal reply target: %w", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.reply_outbox (
    reply_id, logical_reply_id, tenant_id, app_id, binding_id, binding_revision, channel, request_id,
    source_event_id, part_no, revision, operation, reply_kind, target_ref,
    payload, artifact_ref
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (
    tenant_id, app_id, binding_id, request_id, source_event_id,
    logical_reply_id, part_no, revision, operation
) DO NOTHING`,
			effective.ReplyID,
			effective.LogicalReplyID,
			effective.TenantID,
			effective.AppID,
			effective.BindingID,
			effective.BindingRevision,
			effective.Channel,
			effective.RequestID,
			effective.SourceEventID,
			effective.PartNo,
			effective.Revision,
			effective.Operation,
			effective.Kind,
			target,
			payload,
			effective.ArtifactRef,
		); err != nil {
			return fmt.Errorf("insert reply outbox: %w", err)
		}
		if effective.Text != "" {
			state.content = effective.Text
		}
	}
	if _, err := tx.Exec(ctx, `
UPDATE platform.reply_projection_state
SET last_event_seq = $5, content = $6, updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND request_id = $4`,
		tenantID, appID, bindingID, requestID, sequence, state.content); err != nil {
		return fmt.Errorf("advance reply projection state: %w", err)
	}
	return nil
}

func normalizeProjectedReply(
	ctx context.Context,
	tx pgx.Tx,
	reply channels.Reply,
	previousContent string,
) (*channels.Reply, error) {
	if reply.ContentDelta {
		reply.Text = previousContent + reply.Text
		reply.ContentDelta = false
	}
	if reply.Operation == channels.ReplyOperationFinalize && reply.Text == "" {
		reply.Text = previousContent
	}
	if reply.Kind == channels.ReplyKindText || reply.Kind == channels.ReplyKindFallbackText {
		if reply.Text == "" {
			return nil, nil
		}
	}
	if reply.Operation == channels.ReplyOperationSend || reply.Operation == channels.ReplyOperationUpdate || reply.Operation == channels.ReplyOperationFinalize {
		var previous bool
		if err := tx.QueryRow(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM platform.reply_outbox
    WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
      AND request_id = $4 AND logical_reply_id = $5 AND part_no = $6
      AND operation IN ('SEND', 'UPDATE')
)`, reply.TenantID, reply.AppID, reply.BindingID, reply.RequestID, reply.LogicalReplyID, reply.PartNo).Scan(&previous); err != nil {
			return nil, fmt.Errorf("check previous reply operation: %w", err)
		}
		if reply.Operation == channels.ReplyOperationSend && previous {
			reply.Operation = channels.ReplyOperationUpdate
		}
		if reply.Operation == channels.ReplyOperationUpdate && !previous {
			reply.Operation = channels.ReplyOperationSend
		}
	}
	reply.ReplyID = reply.StableID()
	if err := reply.Validate(); err != nil {
		return nil, fmt.Errorf("projected reply: %w", err)
	}
	return &reply, nil
}

// ClaimReplies leases ready replies while preserving multipart and streaming
// operation order within one logical reply.
func (s *Store) ClaimReplies(
	ctx context.Context,
	owner string,
	leaseDuration time.Duration,
	limit int,
) ([]worker.ReplyDelivery, error) {
	if err := s.validate(); err != nil {
		return nil, err
	}
	if owner == "" {
		return nil, errors.New("reply sender owner is required")
	}
	if leaseDuration <= 0 {
		return nil, errors.New("reply lease duration must be positive")
	}
	if limit <= 0 {
		limit = 32
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox o
SET status = 'PERMANENTLY_FAILED', lease_owner = NULL, lease_until = NULL,
    last_error_type = 'binding_changed', last_error = 'binding authorization changed',
    updated_at = clock_timestamp()
WHERE (o.status = 'PENDING' OR (o.status = 'SENDING' AND o.lease_until <= clock_timestamp()))
  AND EXISTS (
      SELECT 1
      FROM platform.channel_binding b
      WHERE b.tenant_id = o.tenant_id
        AND b.app_id = o.app_id
        AND b.binding_id = o.binding_id
        AND (b.channel <> o.channel OR b.binding_revision <> o.binding_revision)
  )`); err != nil {
		return nil, fmt.Errorf("reject stale reply bindings: %w", err)
	}
	rows, err := s.pool.Query(ctx, `
WITH candidates AS (
    SELECT o.reply_id
    FROM platform.reply_outbox o
    WHERE (
        (o.status = 'PENDING' AND o.next_attempt_at <= clock_timestamp())
        OR (o.status = 'SENDING' AND o.lease_until <= clock_timestamp())
    )
    AND EXISTS (
        SELECT 1
        FROM platform.channel_binding b
        WHERE b.tenant_id = o.tenant_id
          AND b.app_id = o.app_id
          AND b.binding_id = o.binding_id
          AND b.status = 'ACTIVE'
          AND b.channel = o.channel
          AND b.binding_revision = o.binding_revision
    )
    AND NOT EXISTS (
        SELECT 1
        FROM platform.reply_outbox p
        WHERE p.tenant_id = o.tenant_id
          AND p.app_id = o.app_id
          AND p.binding_id = o.binding_id
          AND p.request_id = o.request_id
          AND p.logical_reply_id = o.logical_reply_id
          AND p.reply_id <> o.reply_id
          AND (
              p.part_no < o.part_no
              OR (
                  p.part_no = o.part_no
                  AND (
                      p.revision < o.revision
                      OR (
                          p.revision = o.revision
                          AND CASE p.operation WHEN 'SEND' THEN 1 WHEN 'UPDATE' THEN 2 ELSE 3 END
                              < CASE o.operation WHEN 'SEND' THEN 1 WHEN 'UPDATE' THEN 2 ELSE 3 END
                      )
                  )
              )
          )
          AND p.status <> 'SENT'
    )
    ORDER BY o.created_at, o.reply_id
    FOR UPDATE SKIP LOCKED
    LIMIT $3
)
UPDATE platform.reply_outbox o
SET status = 'SENDING', lease_owner = $1,
    lease_until = clock_timestamp() + $2::interval,
    attempt = attempt + 1,
    provider_message_id = CASE
        WHEN o.operation IN ('UPDATE', 'FINALIZE') AND o.provider_message_id = '' THEN
            COALESCE((
                SELECT p.provider_message_id
                FROM platform.reply_outbox p
                WHERE p.tenant_id = o.tenant_id
                  AND p.app_id = o.app_id
                  AND p.binding_id = o.binding_id
                  AND p.request_id = o.request_id
                  AND p.logical_reply_id = o.logical_reply_id
                  AND p.part_no = o.part_no
                  AND p.status = 'SENT'
                  AND p.provider_message_id <> ''
                  AND (
                      p.revision < o.revision
                      OR (
                          p.revision = o.revision
                          AND CASE p.operation WHEN 'SEND' THEN 1 WHEN 'UPDATE' THEN 2 ELSE 3 END
                              < CASE o.operation WHEN 'SEND' THEN 1 WHEN 'UPDATE' THEN 2 ELSE 3 END
                      )
                  )
                ORDER BY p.revision DESC,
                         CASE p.operation WHEN 'SEND' THEN 1 WHEN 'UPDATE' THEN 2 ELSE 3 END DESC
                LIMIT 1
            ), '')
        ELSE o.provider_message_id
    END,
    updated_at = clock_timestamp()
FROM candidates c
WHERE o.reply_id = c.reply_id
RETURNING o.reply_id, o.logical_reply_id, o.tenant_id, o.app_id, o.binding_id,
          o.binding_revision, o.channel, o.request_id, o.source_event_id,
          o.part_no, o.revision, o.operation,
          o.reply_kind, o.target_ref, o.payload, o.artifact_ref, o.attempt,
          o.lease_owner, o.lease_until, o.provider_message_id`,
		owner, intervalLiteral(leaseDuration), limit)
	if err != nil {
		return nil, fmt.Errorf("claim reply outbox: %w", err)
	}
	defer rows.Close()
	result := make([]worker.ReplyDelivery, 0)
	for rows.Next() {
		delivery, err := scanReplyDelivery(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, delivery)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate reply outbox: %w", err)
	}
	return result, nil
}

type replyRowScanner interface {
	Scan(...any) error
}

func scanReplyDelivery(row replyRowScanner) (worker.ReplyDelivery, error) {
	var (
		replyID, logicalReplyID, tenantID, appID, bindingID, channel, requestID    string
		sourceEventID, operation, kind, artifactRef, leaseOwner, providerMessageID string
		bindingRevision, partNo, revision, attempt                                 int64
		targetJSON, payloadJSON                                                    []byte
		leaseUntil                                                                 time.Time
	)
	if err := row.Scan(
		&replyID, &logicalReplyID, &tenantID, &appID, &bindingID, &bindingRevision, &channel, &requestID,
		&sourceEventID, &partNo, &revision, &operation, &kind, &targetJSON,
		&payloadJSON, &artifactRef, &attempt, &leaseOwner, &leaseUntil, &providerMessageID,
	); err != nil {
		return worker.ReplyDelivery{}, fmt.Errorf("scan reply outbox: %w", err)
	}
	var target storedReplyTarget
	if err := json.Unmarshal(targetJSON, &target); err != nil {
		return worker.ReplyDelivery{}, fmt.Errorf("decode reply target: %w", err)
	}
	var payload storedReplyPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return worker.ReplyDelivery{}, fmt.Errorf("decode reply payload: %w", err)
	}
	reply := channels.Reply{
		TenantID:        tenantID,
		AppID:           appID,
		RequestID:       requestID,
		SourceEventID:   sourceEventID,
		Channel:         channels.Channel(channel),
		BindingID:       bindingID,
		BindingRevision: bindingRevision,
		ReplyID:         replyID,
		LogicalReplyID:  logicalReplyID,
		PartNo:          partNo,
		Revision:        revision,
		Operation:       channels.ReplyOperation(operation),
		Kind:            channels.ReplyKind(kind),
		Target: channels.ReplyTarget{
			Kind:             target.Kind,
			InternalEntityID: target.InternalEntityID,
		},
		Text:        payload.Text,
		Card:        payload.Card,
		ArtifactRef: artifactRef,
	}
	return worker.ReplyDelivery{
		Reply:             reply,
		Attempt:           int(attempt),
		LeaseOwner:        leaseOwner,
		LeaseUntil:        leaseUntil.UTC(),
		ProviderMessageID: providerMessageID,
	}, nil
}

// CompleteReply marks one provider call as sent when its sender lease is
// still valid. A repeated completion after SENT is idempotent.
func (s *Store) CompleteReply(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	receipt channels.ProviderReceipt,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = 'SENT', lease_owner = NULL, lease_until = NULL,
    provider_message_id = $3, updated_at = clock_timestamp()
WHERE reply_id = $1 AND status = 'SENDING' AND lease_owner = $2
  AND lease_until > clock_timestamp()`, delivery.Reply.ReplyID, delivery.LeaseOwner, receipt.ProviderMessageID)
	if err != nil {
		return fmt.Errorf("complete reply: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.reply_outbox WHERE reply_id = $1`, delivery.Reply.ReplyID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("complete reply: %w", ErrReplyLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("read reply completion: %w", err)
	}
	if status == "SENT" {
		return nil
	}
	return fmt.Errorf("complete reply: %w", ErrReplyLeaseLost)
}

// RetryReply returns a leased row to PENDING with a bounded retry timestamp.
func (s *Store) RetryReply(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	errorType string,
	delay time.Duration,
	_ error,
) error {
	return s.transitionReplyFailure(ctx, delivery, "PENDING", errorType, delay)
}

// FailReply retains an unrecoverable reply failure for audit and operator
// inspection; it never removes the row.
func (s *Store) FailReply(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	errorType string,
	_ error,
) error {
	return s.transitionReplyFailure(ctx, delivery, "PERMANENTLY_FAILED", errorType, 0)
}

func (s *Store) transitionReplyFailure(
	ctx context.Context,
	delivery worker.ReplyDelivery,
	status, errorType string,
	delay time.Duration,
) error {
	if err := s.validate(); err != nil {
		return err
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	if errorType == "" {
		errorType = "provider_send"
	}
	if delay < 0 {
		return errors.New("reply retry delay must not be negative")
	}
	tag, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = $3,
    next_attempt_at = clock_timestamp() + $4::interval,
    lease_owner = NULL, lease_until = NULL,
    last_error_type = $5, last_error = $5,
    updated_at = clock_timestamp()
WHERE reply_id = $1 AND status = 'SENDING' AND lease_owner = $2
  AND lease_until > clock_timestamp()`, delivery.Reply.ReplyID, delivery.LeaseOwner, status,
		intervalLiteral(delay), errorType)
	if err != nil {
		return fmt.Errorf("transition reply failure: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var current string
	err = s.pool.QueryRow(ctx, `SELECT status FROM platform.reply_outbox WHERE reply_id = $1`, delivery.Reply.ReplyID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("transition reply failure: %w", ErrReplyLeaseLost)
	}
	if err != nil {
		return fmt.Errorf("read reply failure transition: %w", err)
	}
	if current == "SENT" || current == status {
		return nil
	}
	return fmt.Errorf("transition reply failure: %w", ErrReplyLeaseLost)
}

// RecoverReplyLeases makes expired SENDING rows claimable again.
func (s *Store) RecoverReplyLeases(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	if _, err := s.pool.Exec(ctx, `
UPDATE platform.reply_outbox
SET status = 'PENDING', lease_owner = NULL, lease_until = NULL,
    updated_at = clock_timestamp()
WHERE status = 'SENDING' AND lease_until <= clock_timestamp()`); err != nil {
		return fmt.Errorf("recover reply leases: %w", err)
	}
	return nil
}

// ResolveReplyTarget opens the scoped provider target only for the immediate
// outbound operation. A present but expired message target is terminal and is
// never replaced with a different entity target.
func (s *Store) ResolveReplyTarget(
	ctx context.Context,
	delivery worker.ReplyDelivery,
) (string, channels.OutboundContext, error) {
	if err := s.validate(); err != nil {
		return "", channels.OutboundContext{}, err
	}
	if err := delivery.Validate(); err != nil {
		return "", channels.OutboundContext{}, err
	}
	if s.identityMapper == nil || s.identityMapper.protector == nil {
		return "", channels.OutboundContext{}, errors.New("target protector is required")
	}
	reply := delivery.Reply
	var bindingStatus string
	var bindingChannel string
	var bindingRevision int64
	if err := s.pool.QueryRow(ctx, `
SELECT status, channel, binding_revision
FROM platform.channel_binding
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		reply.TenantID, reply.AppID, reply.BindingID,
	).Scan(&bindingStatus, &bindingChannel, &bindingRevision); err != nil {
		return "", channels.OutboundContext{}, resolveError("reply binding", err)
	}
	if bindingStatus != string(channels.BindingActive) {
		return "", channels.OutboundContext{}, channels.ErrBindingInactive
	}
	if channels.Channel(bindingChannel) != reply.Channel || bindingRevision != reply.BindingRevision {
		return "", channels.OutboundContext{}, errors.New("reply binding authorization changed")
	}
	var targetJSON []byte
	var expiresAt *time.Time
	err := s.pool.QueryRow(ctx, `
SELECT provider_reply_target_envelope, reply_target_expires_at
FROM platform.channel_inbox
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND request_id = $4
  AND provider_reply_target_envelope IS NOT NULL
ORDER BY created_at
LIMIT 1`, reply.TenantID, reply.AppID, reply.BindingID, reply.RequestID).Scan(&targetJSON, &expiresAt)
	if err == nil {
		if expiresAt == nil || !time.Now().UTC().Before(expiresAt.UTC()) {
			return "", channels.OutboundContext{}, ErrReplyTargetExpired
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope:            tenant.Scope{TenantID: reply.TenantID, AppID: reply.AppID},
			BindingID:        reply.BindingID,
			Channel:          reply.Channel,
			EntityType:       channels.TargetEntityInbox,
			InternalEntityID: reply.RequestID,
		}, channels.TargetPurposeReplyMessage, targetJSON)
		if err != nil {
			return "", channels.OutboundContext{}, err
		}
		return plaintext.ProviderTarget, channels.OutboundContext{}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", channels.OutboundContext{}, fmt.Errorf("find message reply target: %w", err)
	}
	scope := tenant.Scope{TenantID: reply.TenantID, AppID: reply.AppID}
	switch reply.Target.Kind {
	case channels.TargetKindUser:
		err = s.pool.QueryRow(ctx, `
SELECT provider_target_envelope
FROM platform.channel_identity
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND user_id = $4`,
			reply.TenantID, reply.AppID, reply.BindingID, reply.Target.InternalEntityID).Scan(&targetJSON)
		if err != nil {
			return "", channels.OutboundContext{}, resolveError("reply identity target", err)
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope: scope, BindingID: reply.BindingID, Channel: reply.Channel,
			EntityType: channels.TargetEntityIdentity, InternalEntityID: reply.Target.InternalEntityID,
		}, channels.TargetPurposeIdentityUser, targetJSON)
		if err != nil {
			return "", channels.OutboundContext{}, err
		}
		return plaintext.ProviderTarget, channels.OutboundContext{}, nil
	case channels.TargetKindConversation, channels.TargetKindTopic:
		err = s.pool.QueryRow(ctx, `
SELECT provider_target_envelope, scope
FROM platform.channel_conversation
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND conversation_id = $4`,
			reply.TenantID, reply.AppID, reply.BindingID, reply.Target.InternalEntityID).Scan(&targetJSON, new(string))
		if err != nil {
			return "", channels.OutboundContext{}, resolveError("reply conversation target", err)
		}
		purpose := channels.TargetPurposeConversationChat
		if reply.Target.Kind == channels.TargetKindTopic {
			purpose = channels.TargetPurposeConversationTopic
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope: scope, BindingID: reply.BindingID, Channel: reply.Channel,
			EntityType: channels.TargetEntityConversation, InternalEntityID: reply.Target.InternalEntityID,
		}, purpose, targetJSON)
		if err != nil {
			return "", channels.OutboundContext{}, err
		}
		return plaintext.ProviderTarget, channels.OutboundContext{}, nil
	default:
		return "", channels.OutboundContext{}, errors.New("reply target kind is unsupported")
	}
}

func (s *Store) openReplyTarget(
	ctx context.Context,
	reply channels.Reply,
	targetContext channels.TargetContext,
	purpose channels.TargetPurpose,
	encoded []byte,
) (channels.TargetPlaintext, error) {
	var envelope channels.TargetEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("decode reply target envelope: %w", err)
	}
	if err := envelope.Validate(); err != nil {
		return channels.TargetPlaintext{}, err
	}
	plaintext, err := s.identityMapper.protector.Open(ctx, targetContext, purpose, envelope)
	if err != nil {
		return channels.TargetPlaintext{}, fmt.Errorf("open reply target: %w", err)
	}
	if plaintext.Channel != reply.Channel {
		return channels.TargetPlaintext{}, errors.New("reply target channel does not match")
	}
	if err := plaintext.Validate(purpose); err != nil {
		return channels.TargetPlaintext{}, err
	}
	return plaintext, nil
}

func replyEventSequence(sourceEventID, requestID string) (int64, error) {
	prefix := requestID + ":"
	if !strings.HasPrefix(sourceEventID, prefix) {
		return 0, errors.New("reply source event id is invalid")
	}
	value, err := strconv.ParseInt(strings.TrimPrefix(sourceEventID, prefix), 10, 64)
	if err != nil || value <= 0 {
		return 0, errors.New("reply source event sequence is invalid")
	}
	return value, nil
}

var (
	_ worker.ReplyOutbox         = (*Store)(nil)
	_ worker.ReplyTargetResolver = (*Store)(nil)
)
