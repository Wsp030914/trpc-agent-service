package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

const (
	inboundArtifactPending  = "PENDING"
	inboundArtifactAttached = "ATTACHED"
	inboundArtifactDeleted  = "DELETED"
)

// StagedInboundArtifact is durable media written before channel admission.
// The provider reference never enters this record; ArtifactRef is an
// internally generated name that can be atomically attached to a session.
type StagedInboundArtifact struct {
	TenantID          string
	AppID             string
	BindingID         string
	ExternalMessageID string
	ItemNo            int
	ArtifactRef       string
	ConfigVersion     string
	Filename          string
	ObjectKey         string
	MIMEType          string
	Size              int64
	Status            string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// StageInboundArtifact records one uploaded provider attachment using the
// binding/message/item idempotency tuple. A concurrent duplicate returns the
// original durable object instead of replacing it.
func (s *Store) StageInboundArtifact(
	ctx context.Context,
	record StagedInboundArtifact,
) (StagedInboundArtifact, error) {
	if err := s.validate(); err != nil {
		return StagedInboundArtifact{}, err
	}
	if err := validateStagedInboundArtifact(record); err != nil {
		return StagedInboundArtifact{}, err
	}
	_, err := s.pool.Exec(ctx, `
INSERT INTO platform.inbound_artifact (
	    tenant_id, app_id, binding_id, external_message_id, item_no,
	    artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
	    status
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'PENDING')
ON CONFLICT (tenant_id, app_id, binding_id, external_message_id, item_no) DO NOTHING`,
		record.TenantID,
		record.AppID,
		record.BindingID,
		record.ExternalMessageID,
		record.ItemNo,
		record.ArtifactRef,
		record.ConfigVersion,
		record.Filename,
		record.ObjectKey,
		record.MIMEType,
		record.Size,
	)
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("stage inbound artifact: %w", err)
	}
	staged, err := s.findStagedInboundArtifact(ctx, nil, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
	if err != nil {
		return StagedInboundArtifact{}, err
	}
	if staged.Status == inboundArtifactDeleted {
		return StagedInboundArtifact{}, errors.New("staged inbound artifact was deleted")
	}
	return staged, nil
}

func validateStagedInboundArtifact(record StagedInboundArtifact) error {
	if record.TenantID == "" || record.AppID == "" || record.BindingID == "" ||
		record.ExternalMessageID == "" || record.ConfigVersion == "" {
		return errors.New("staged inbound artifact scope and config are required")
	}
	if record.ItemNo < 0 {
		return errors.New("staged inbound artifact item number must not be negative")
	}
	if _, err := inboundArtifactName(record.ArtifactRef); err != nil {
		return errors.New("staged inbound artifact ref is invalid")
	}
	if record.Filename == "" || record.ObjectKey == "" {
		return errors.New("staged inbound artifact filename and object key are required")
	}
	if strings.ContainsAny(record.Filename+record.MIMEType, "\r\n\x00") {
		return errors.New("staged inbound artifact metadata is invalid")
	}
	if record.Size <= 0 {
		return errors.New("staged inbound artifact size must be positive")
	}
	return nil
}

func (s *Store) findStagedInboundArtifact(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, externalMessageID string, itemNo int,
) (StagedInboundArtifact, error) {
	query := `
SELECT tenant_id, app_id, binding_id, external_message_id, item_no,
       artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
       status, created_at, updated_at
FROM platform.inbound_artifact
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5`
	var row pgx.Row
	if tx != nil {
		row = tx.QueryRow(ctx, query+" FOR UPDATE", tenantID, appID, bindingID, externalMessageID, itemNo)
	} else {
		row = s.pool.QueryRow(ctx, query, tenantID, appID, bindingID, externalMessageID, itemNo)
	}
	record, err := scanStagedInboundArtifact(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return StagedInboundArtifact{}, fmt.Errorf("staged inbound artifact: %w", ErrNotFound)
	}
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("find staged inbound artifact: %w", err)
	}
	return record, nil
}

// MarkInboundArtifactDeleted moves a staged object out of PENDING under a row
// lock. It is idempotent for a previously deleted row and deliberately does
// nothing for ATTACHED rows, which are now owned by a committed execution.
func (s *Store) MarkInboundArtifactDeleted(
	ctx context.Context,
	record StagedInboundArtifact,
) (StagedInboundArtifact, bool, error) {
	if err := s.validate(); err != nil {
		return StagedInboundArtifact{}, false, err
	}
	if record.TenantID == "" || record.AppID == "" || record.BindingID == "" ||
		record.ExternalMessageID == "" || record.ArtifactRef == "" || record.ConfigVersion == "" {
		return StagedInboundArtifact{}, false, errors.New("inbound artifact compensation scope is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return StagedInboundArtifact{}, false, fmt.Errorf("begin inbound artifact compensation: %w", err)
	}
	defer func() { rollback(tx) }()
	staged, err := s.findStagedInboundArtifact(ctx, tx, record.TenantID, record.AppID, record.BindingID, record.ExternalMessageID, record.ItemNo)
	if errors.Is(err, ErrNotFound) {
		return StagedInboundArtifact{}, false, nil
	}
	if err != nil {
		return StagedInboundArtifact{}, false, err
	}
	if staged.ArtifactRef != record.ArtifactRef || staged.ConfigVersion != record.ConfigVersion {
		return StagedInboundArtifact{}, false, errors.New("inbound artifact compensation identity mismatch")
	}
	if staged.Status == inboundArtifactAttached {
		if err := tx.Commit(ctx); err != nil {
			return StagedInboundArtifact{}, false, fmt.Errorf("commit attached artifact compensation check: %w", err)
		}
		return staged, false, nil
	}
	if staged.Status != inboundArtifactPending && staged.Status != inboundArtifactDeleted {
		return StagedInboundArtifact{}, false, fmt.Errorf("inbound artifact status %q cannot be compensated", staged.Status)
	}
	if staged.Status == inboundArtifactPending {
		if _, err := tx.Exec(ctx, `
UPDATE platform.inbound_artifact
SET status = 'DELETED', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND item_no = $5
  AND artifact_ref = $6 AND status = 'PENDING'`,
			staged.TenantID, staged.AppID, staged.BindingID, staged.ExternalMessageID,
			staged.ItemNo, staged.ArtifactRef,
		); err != nil {
			return StagedInboundArtifact{}, false, fmt.Errorf("mark inbound artifact deleted: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return StagedInboundArtifact{}, false, fmt.Errorf("commit inbound artifact compensation: %w", err)
	}
	return staged, true, nil
}

func scanStagedInboundArtifact(row interface{ Scan(...any) error }) (StagedInboundArtifact, error) {
	var record StagedInboundArtifact
	err := row.Scan(
		&record.TenantID,
		&record.AppID,
		&record.BindingID,
		&record.ExternalMessageID,
		&record.ItemNo,
		&record.ArtifactRef,
		&record.ConfigVersion,
		&record.Filename,
		&record.ObjectKey,
		&record.MIMEType,
		&record.Size,
		&record.Status,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if err != nil {
		return StagedInboundArtifact{}, err
	}
	return record, nil
}

func attachStagedInboundArtifacts(
	ctx context.Context,
	tx pgx.Tx,
	input channels.ChannelInput,
	configVersion, sessionPrincipalID, sessionID string,
) error {
	if len(input.ArtifactRefs) == 0 {
		return nil
	}
	for _, artifactRef := range input.ArtifactRefs {
		name, err := inboundArtifactName(artifactRef)
		if err != nil {
			return err
		}
		staged, err := findStagedInboundArtifactTx(
			ctx,
			tx,
			input.TenantID,
			input.AppID,
			input.BindingID,
			input.ExternalMessageID,
			artifactRef,
		)
		if err != nil {
			return err
		}
		if staged.ConfigVersion != configVersion {
			return errors.New("inbound artifact config version changed before admission")
		}
		if staged.Status != inboundArtifactPending {
			return fmt.Errorf("staged inbound artifact status %q is not pending", staged.Status)
		}
		if _, err := tx.Exec(ctx, `
INSERT INTO platform.artifact (
    artifact_id, tenant_id, app_id, session_principal_id, session_id,
    filename, version, object_key, mime_type, size_bytes, status, config_version
) VALUES (gen_random_uuid()::text, $1, $2, $3, $4, $5, 0, $6, $7, $8, 'AVAILABLE', $9)`,
			input.TenantID,
			input.AppID,
			sessionPrincipalID,
			sessionID,
			name,
			staged.ObjectKey,
			staged.MIMEType,
			staged.Size,
			staged.ConfigVersion,
		); err != nil {
			return fmt.Errorf("attach inbound artifact metadata: %w", err)
		}
		result, err := tx.Exec(ctx, `
UPDATE platform.inbound_artifact
SET status = 'ATTACHED', updated_at = clock_timestamp()
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND artifact_ref = $5 AND status = 'PENDING'`,
			input.TenantID, input.AppID, input.BindingID, input.ExternalMessageID, artifactRef,
		)
		if err != nil {
			return fmt.Errorf("attach inbound artifact stage: %w", err)
		}
		if result.RowsAffected() != 1 {
			return errors.New("staged inbound artifact changed during admission")
		}
	}
	return nil
}

func findStagedInboundArtifactTx(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID, externalMessageID, artifactRef string,
) (StagedInboundArtifact, error) {
	var record StagedInboundArtifact
	err := tx.QueryRow(ctx, `
	SELECT tenant_id, app_id, binding_id, external_message_id, item_no,
	       artifact_ref, config_version, filename, object_key, mime_type, size_bytes,
	       status, created_at, updated_at
FROM platform.inbound_artifact
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
  AND external_message_id = $4 AND artifact_ref = $5
FOR UPDATE`, tenantID, appID, bindingID, externalMessageID, artifactRef).Scan(
		&record.TenantID,
		&record.AppID,
		&record.BindingID,
		&record.ExternalMessageID,
		&record.ItemNo,
		&record.ArtifactRef,
		&record.ConfigVersion,
		&record.Filename,
		&record.ObjectKey,
		&record.MIMEType,
		&record.Size,
		&record.Status,
		&record.CreatedAt,
		&record.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return StagedInboundArtifact{}, errors.New("inbound artifact was not staged by the verified binding")
	}
	if err != nil {
		return StagedInboundArtifact{}, fmt.Errorf("find inbound artifact for admission: %w", err)
	}
	return record, nil
}

func inboundArtifactName(artifactRef string) (string, error) {
	name, version, err := gateway.ParseArtifactRef(artifactRef)
	if err != nil {
		return "", fmt.Errorf("inbound artifact ref: %w", err)
	}
	if version != 0 {
		return "", errors.New("inbound artifact ref version must be zero")
	}
	return name, nil
}
