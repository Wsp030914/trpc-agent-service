CREATE SCHEMA IF NOT EXISTS platform;

CREATE TABLE platform.tenant (
    tenant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL CHECK (name <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    audit_policy JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (jsonb_typeof(audit_policy) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE platform.agent_app (
    tenant_id TEXT NOT NULL REFERENCES platform.tenant (tenant_id), app_id TEXT NOT NULL CHECK (app_id <> ''),
    name TEXT NOT NULL CHECK (name <> ''), active_config_version TEXT NOT NULL CHECK (active_config_version <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id)
);
CREATE TABLE platform.app_config_version (
    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, version TEXT NOT NULL CHECK (version <> ''),
    model_config JSONB NOT NULL CHECK (jsonb_typeof(model_config) = 'object'),
    tool_policy JSONB NOT NULL CHECK (jsonb_typeof(tool_policy) = 'object'),
    backend_config JSONB NOT NULL CHECK (jsonb_typeof(backend_config) = 'object'),
    audit_policy JSONB NOT NULL CHECK (jsonb_typeof(audit_policy) = 'object'),
    secret_refs JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (jsonb_typeof(secret_refs) IN ('array', 'null')),
    channel_binding_ids JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (jsonb_typeof(channel_binding_ids) IN ('array', 'null')),
    status TEXT NOT NULL DEFAULT 'PUBLISHED' CHECK (status = 'PUBLISHED'), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, version), FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);
ALTER TABLE platform.agent_app ADD CONSTRAINT agent_app_active_config_fk FOREIGN KEY (tenant_id, app_id, active_config_version)
    REFERENCES platform.app_config_version (tenant_id, app_id, version) DEFERRABLE INITIALLY DEFERRED;
CREATE FUNCTION platform.reject_app_config_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'published app config versions are immutable' USING ERRCODE = '55000'; END;
$$;
CREATE TRIGGER app_config_version_immutable BEFORE UPDATE OR DELETE ON platform.app_config_version
    FOR EACH ROW EXECUTE FUNCTION platform.reject_app_config_mutation();

CREATE TABLE platform.api_credential (
    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, credential_id TEXT NOT NULL CHECK (credential_id <> ''),
    key_digest BYTEA NOT NULL UNIQUE CHECK (octet_length(key_digest) = 32), key_prefix TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED', 'REVOKED')), expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), last_used_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, app_id, credential_id), FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);
CREATE FUNCTION platform.reject_credential_reactivation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status = 'REVOKED' AND NEW.status <> 'REVOKED' THEN
        RAISE EXCEPTION 'revoked api credential cannot be reactivated' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER api_credential_revocation_immutable BEFORE UPDATE ON platform.api_credential
    FOR EACH ROW EXECUTE FUNCTION platform.reject_credential_reactivation();
CREATE TABLE platform.channel_binding (
    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, binding_id TEXT NOT NULL CHECK (binding_id <> ''),
    channel TEXT NOT NULL CHECK (channel <> ''), external_account TEXT NOT NULL CHECK (external_account <> ''),
    webhook_url TEXT NOT NULL DEFAULT '', token_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
    signing_secret_ref JSONB NOT NULL DEFAULT '{}'::JSONB, secret_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id), FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);
CREATE TABLE platform.session_lane (
    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''), next_turn_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_turn_seq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, session_principal_id, session_id), FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);
CREATE TABLE platform.execution (
    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, request_id TEXT NOT NULL CHECK (request_id <> ''),
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''), session_id TEXT NOT NULL CHECK (session_id <> ''), user_id TEXT NOT NULL CHECK (user_id <> ''),
    turn_seq BIGINT NOT NULL CHECK (turn_seq > 0), config_version TEXT NOT NULL CHECK (config_version <> ''),
    tenant_source TEXT NOT NULL CHECK (tenant_source IN ('authenticated_claims', 'verified_channel_binding')), source_id TEXT NOT NULL CHECK (source_id <> ''),
    idempotency_key TEXT NOT NULL CHECK (idempotency_key <> ''), payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    command JSONB NOT NULL CHECK (jsonb_typeof(command) = 'object'),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0), next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT, run_token TEXT, lease_until TIMESTAMPTZ, last_error TEXT NOT NULL DEFAULT '', trace_id TEXT NOT NULL,
    started_at TIMESTAMPTZ, finished_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, request_id),
    UNIQUE (tenant_id, app_id, tenant_source, source_id, idempotency_key),
    UNIQUE (tenant_id, app_id, session_principal_id, session_id, turn_seq),
    FOREIGN KEY (tenant_id, app_id, session_principal_id, session_id) REFERENCES platform.session_lane (tenant_id, app_id, session_principal_id, session_id),
    FOREIGN KEY (tenant_id, app_id, config_version) REFERENCES platform.app_config_version (tenant_id, app_id, version)
);
CREATE INDEX execution_active_lane_idx ON platform.execution (tenant_id, app_id, session_principal_id, session_id, turn_seq) WHERE status IN ('PENDING', 'RUNNING');
CREATE INDEX execution_trace_idx ON platform.execution (tenant_id, app_id, trace_id);
CREATE TABLE platform.dispatch_outbox (
    outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, request_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHING', 'SENT', 'CONSUMED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0), next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT, lease_until TIMESTAMPTZ, last_error TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, request_id) REFERENCES platform.execution (tenant_id, app_id, request_id)
);
CREATE INDEX dispatch_outbox_publish_idx ON platform.dispatch_outbox (status, next_attempt_at, outbox_id);
CREATE TABLE platform.execution_event (
    tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, request_id TEXT NOT NULL, event_seq BIGINT NOT NULL CHECK (event_seq > 0),
    event_type TEXT NOT NULL CHECK (event_type <> ''), payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'), created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, request_id, event_seq), FOREIGN KEY (tenant_id, app_id, request_id) REFERENCES platform.execution (tenant_id, app_id, request_id)
);
CREATE TABLE platform.audit_event (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, request_id TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL, session_principal_id TEXT NOT NULL, session_id TEXT NOT NULL, trace_id TEXT NOT NULL,
    agent_name TEXT NOT NULL, event_type TEXT NOT NULL, tool_name TEXT NOT NULL DEFAULT '', decision TEXT NOT NULL DEFAULT '', latency_ms BIGINT NOT NULL DEFAULT 0,
    error_type TEXT NOT NULL DEFAULT '', expire_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, request_id) REFERENCES platform.execution (tenant_id, app_id, request_id)
);
CREATE INDEX audit_event_scope_idx ON platform.audit_event (tenant_id, app_id, created_at);
