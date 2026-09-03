package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

var (
	// ErrReplyLeaseLost means a sender no longer owns the Reply Outbox row.
	ErrReplyLeaseLost = errors.New("reply outbox lease is lost")
	// ErrReplyTargetExpired means a message-scoped provider target cannot be
	// replaced with a stable user or conversation target.
	ErrReplyTargetExpired = errors.New("reply target is expired")
)

type storedReplyPayload struct {
	Text string `json:"text"`
}

type storedReplyTarget struct {
	Kind             channels.TargetKind `json:"target_kind"`
	InternalEntityID string              `json:"internal_entity_id"`
}

func insertReplyOutboxTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, requestID string,
	replies []channels.Reply,
) error {
	for _, reply := range replies {
		if reply.TenantID != tenantID || reply.AppID != appID || reply.BindingID != bindingID || reply.RequestID != requestID {
			return errors.New("reply projection scope does not match state")
		}
		reply.ReplyID = reply.StableID()
		if err := reply.Validate(); err != nil {
			return fmt.Errorf("projected reply: %w", err)
		}
		payload, err := json.Marshal(storedReplyPayload{Text: reply.Text})
		if err != nil {
			return fmt.Errorf("marshal reply payload: %w", err)
		}
		target, err := json.Marshal(storedReplyTarget{
			Kind:             reply.Target.Kind,
			InternalEntityID: reply.Target.InternalEntityID,
		})
		if err != nil {
			return fmt.Errorf("marshal reply target: %w", err)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.reply_outbox (
    reply_id, tenant_id, app_id, binding_id, binding_revision, channel, request_id,
    source_event_id, revision, target_ref, payload
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (tenant_id, app_id, binding_id, request_id, source_event_id, revision)
DO NOTHING`,
			reply.ReplyID,
			reply.TenantID,
			reply.AppID,
			reply.BindingID,
			reply.BindingRevision,
			reply.Channel,
			reply.RequestID,
			reply.SourceEventID,
			reply.Revision,
			target,
			payload,
		); err != nil {
			return fmt.Errorf("insert reply outbox: %w", err)
		}
	}
	return nil
}

// ClaimReplies leases ready replies in event order within one request.
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
          AND p.revision < o.revision
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
    updated_at = clock_timestamp()
FROM candidates c
WHERE o.reply_id = c.reply_id
RETURNING o.reply_id, o.tenant_id, o.app_id, o.binding_id,
          o.binding_revision, o.channel, o.request_id, o.source_event_id,
          o.revision, o.target_ref, o.payload, o.attempt,
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
		replyID, tenantID, appID, bindingID, channel, requestID string
		sourceEventID, leaseOwner, providerMessageID            string
		bindingRevision, revision, attempt                      int64
		targetJSON, payloadJSON                                 []byte
		leaseUntil                                              time.Time
	)
	if err := row.Scan(
		&replyID, &tenantID, &appID, &bindingID, &bindingRevision, &channel, &requestID,
		&sourceEventID, &revision, &targetJSON, &payloadJSON, &attempt, &leaseOwner, &leaseUntil,
		&providerMessageID,
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
		Revision:        revision,
		Target: channels.ReplyTarget{
			Kind:             target.Kind,
			InternalEntityID: target.InternalEntityID,
		},
		Text: payload.Text,
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

// FailReply retains an unrecoverable reply failure for operator inspection.
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
) (string, error) {
	if err := s.validate(); err != nil {
		return "", err
	}
	if err := delivery.Validate(); err != nil {
		return "", err
	}
	if s.identityMapper == nil || s.identityMapper.protector == nil {
		return "", errors.New("target protector is required")
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
		return "", resolveError("reply binding", err)
	}
	if bindingStatus != string(channels.BindingActive) {
		return "", channels.ErrBindingInactive
	}
	if channels.Channel(bindingChannel) != reply.Channel || bindingRevision != reply.BindingRevision {
		return "", errors.New("reply binding authorization changed")
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
			return "", ErrReplyTargetExpired
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope:            tenant.Scope{TenantID: reply.TenantID, AppID: reply.AppID},
			BindingID:        reply.BindingID,
			Channel:          reply.Channel,
			EntityType:       channels.TargetEntityInbox,
			InternalEntityID: reply.RequestID,
		}, channels.TargetPurposeReplyMessage, targetJSON)
		if err != nil {
			return "", err
		}
		return plaintext.ProviderTarget, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("find message reply target: %w", err)
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
			return "", resolveError("reply identity target", err)
		}
		plaintext, err := s.openReplyTarget(ctx, reply, channels.TargetContext{
			Scope: scope, BindingID: reply.BindingID, Channel: reply.Channel,
			EntityType: channels.TargetEntityIdentity, InternalEntityID: reply.Target.InternalEntityID,
		}, channels.TargetPurposeIdentityUser, targetJSON)
		if err != nil {
			return "", err
		}
		return plaintext.ProviderTarget, nil
	case channels.TargetKindConversation, channels.TargetKindTopic:
		err = s.pool.QueryRow(ctx, `
SELECT provider_target_envelope, scope
FROM platform.channel_conversation
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3 AND conversation_id = $4`,
			reply.TenantID, reply.AppID, reply.BindingID, reply.Target.InternalEntityID).Scan(&targetJSON, new(string))
		if err != nil {
			return "", resolveError("reply conversation target", err)
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
			return "", err
		}
		return plaintext.ProviderTarget, nil
	default:
		return "", errors.New("reply target kind is unsupported")
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

var (
	_ worker.ReplyOutbox = (*Store)(nil)
)
