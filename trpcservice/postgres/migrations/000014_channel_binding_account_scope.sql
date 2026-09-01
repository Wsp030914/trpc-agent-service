ALTER TABLE platform.channel_binding
    ADD COLUMN external_account_scope TEXT NOT NULL DEFAULT '';

CREATE OR REPLACE FUNCTION platform.bump_channel_binding_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.binding_id IS DISTINCT FROM OLD.binding_id THEN
        RAISE EXCEPTION 'channel binding scope is immutable' USING ERRCODE = '55000';
    END IF;

    IF NEW.channel IS DISTINCT FROM OLD.channel
       OR NEW.external_account IS DISTINCT FROM OLD.external_account
       OR NEW.external_account_scope IS DISTINCT FROM OLD.external_account_scope
       OR NEW.webhook_url IS DISTINCT FROM OLD.webhook_url
       OR NEW.token_ref IS DISTINCT FROM OLD.token_ref
       OR NEW.signing_secret_ref IS DISTINCT FROM OLD.signing_secret_ref
       OR NEW.secret_ref IS DISTINCT FROM OLD.secret_ref
       OR NEW.status IS DISTINCT FROM OLD.status
       OR NEW.public_route_id IS DISTINCT FROM OLD.public_route_id THEN
        NEW.binding_revision := OLD.binding_revision + 1;
    ELSE
        NEW.binding_revision := OLD.binding_revision;
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;
