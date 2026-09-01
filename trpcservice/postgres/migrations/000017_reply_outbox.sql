CREATE TABLE platform.reply_outbox (
    reply_id TEXT PRIMARY KEY CHECK (reply_id <> ''),
    logical_reply_id TEXT NOT NULL CHECK (logical_reply_id <> ''),
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel <> ''),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    source_event_id TEXT NOT NULL CHECK (source_event_id <> ''),
    part_no BIGINT NOT NULL CHECK (part_no > 0),
    revision BIGINT NOT NULL CHECK (revision > 0),
    operation TEXT NOT NULL CHECK (operation IN ('SEND', 'UPDATE', 'FINALIZE')),
    reply_kind TEXT NOT NULL CHECK (reply_kind IN ('text', 'card', 'artifact', 'fallback_text')),
    target_ref JSONB NOT NULL CHECK (jsonb_typeof(target_ref) = 'object'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    artifact_ref TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'PERMANENTLY_FAILED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    provider_message_id TEXT NOT NULL DEFAULT '',
    last_error_type TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    FOREIGN KEY (tenant_id, app_id, request_id)
        REFERENCES platform.execution (tenant_id, app_id, request_id),
    CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    ),
    UNIQUE (
        tenant_id, app_id, binding_id, request_id, source_event_id,
        logical_reply_id, part_no, revision, operation
    )
);

CREATE INDEX reply_outbox_claim_idx
    ON platform.reply_outbox (status, next_attempt_at, created_at, reply_id);

CREATE INDEX reply_outbox_order_idx
    ON platform.reply_outbox (
        tenant_id, app_id, binding_id, request_id, logical_reply_id, part_no, operation
    );
