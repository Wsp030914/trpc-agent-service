package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// ResolveBindingByPublicRoute locates one Binding by its opaque public route.
// The route is only a locator; callers must still validate channel and status
// before using the returned snapshot as a trusted source.
func (s *Store) ResolveBindingByPublicRoute(
	ctx context.Context,
	channel channels.Channel,
	publicRouteID string,
) (channels.BindingSnapshot, error) {
	if err := s.validate(); err != nil {
		return channels.BindingSnapshot{}, err
	}
	if err := channel.Validate(); err != nil {
		return channels.BindingSnapshot{}, err
	}
	if err := channels.ValidatePublicRouteID(publicRouteID); err != nil {
		return channels.BindingSnapshot{}, err
	}
	binding, err := scanChannelBinding(s.pool.QueryRow(ctx, channelBindingSelect+`
WHERE public_route_id = $1`, publicRouteID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return channels.BindingSnapshot{}, fmt.Errorf("%w: %w", channels.ErrBindingNotFound, ErrNotFound)
		}
		return channels.BindingSnapshot{}, fmt.Errorf("resolve channel binding by public route: %w", err)
	}
	if binding.Channel != channel {
		return channels.BindingSnapshot{}, channels.ErrBindingChannelMismatch
	}
	return binding.Snapshot(), nil
}

// RotateChannelBindingRoute replaces a binding's public route. The database
// trigger advances binding_revision atomically with the route update, and the
// old route is invalid immediately after commit.
func (s *Store) RotateChannelBindingRoute(
	ctx context.Context,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	if err := s.validate(); err != nil {
		return channels.Binding{}, err
	}
	if tenantID == "" {
		return channels.Binding{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return channels.Binding{}, errors.New("app_id is required")
	}
	if bindingID == "" {
		return channels.Binding{}, errors.New("binding_id is required")
	}
	publicRouteID, err := channels.NewPublicRouteID()
	if err != nil {
		return channels.Binding{}, err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return channels.Binding{}, fmt.Errorf("begin channel binding route rotation: %w", err)
	}
	defer func() {
		rollback(tx)
	}()

	if _, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
FOR UPDATE`, tenantID, appID, bindingID)); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return channels.Binding{}, fmt.Errorf("channel binding: %w", ErrNotFound)
		}
		return channels.Binding{}, fmt.Errorf("lock channel binding for route rotation: %w", err)
	}
	if _, err := tx.Exec(
		ctx,
		`UPDATE platform.channel_binding
SET public_route_id = $4
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`,
		tenantID,
		appID,
		bindingID,
		publicRouteID,
	); err != nil {
		return channels.Binding{}, fmt.Errorf("rotate channel binding route: %w", err)
	}
	rotated, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3`, tenantID, appID, bindingID))
	if err != nil {
		return channels.Binding{}, fmt.Errorf("read rotated channel binding: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return channels.Binding{}, fmt.Errorf("commit channel binding route rotation: %w", err)
	}
	return rotated, nil
}

func lockChannelBinding(
	ctx context.Context,
	tx pgx.Tx,
	tenantID, appID, bindingID string,
) (channels.Binding, error) {
	binding, err := scanChannelBinding(tx.QueryRow(ctx, channelBindingSelect+`
WHERE tenant_id = $1 AND app_id = $2 AND binding_id = $3
FOR UPDATE`, tenantID, appID, bindingID))
	if err != nil {
		return channels.Binding{}, resolveError("channel binding", err)
	}
	return binding, nil
}

const channelBindingSelect = `SELECT
    tenant_id,
    app_id,
    binding_id,
    channel,
    external_account,
    external_account_scope,
    webhook_url,
    token_ref,
    signing_secret_ref,
    secret_ref,
    public_route_id,
    binding_revision,
    status
FROM platform.channel_binding`

func scanChannelBinding(row pgx.Row) (channels.Binding, error) {
	var binding channels.Binding
	var tokenRef, signingSecretRef, secret []byte
	err := row.Scan(
		&binding.TenantID,
		&binding.AppID,
		&binding.BindingID,
		&binding.Channel,
		&binding.ExternalAccount,
		&binding.ExternalAccountScope,
		&binding.WebhookURL,
		&tokenRef,
		&signingSecretRef,
		&secret,
		&binding.PublicRouteID,
		&binding.BindingRevision,
		&binding.Status,
	)
	if err != nil {
		return channels.Binding{}, err
	}
	if err := unmarshalBindingSecretRefs(&binding, tokenRef, signingSecretRef, secret); err != nil {
		return channels.Binding{}, err
	}
	return binding, nil
}
