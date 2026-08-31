CREATE TABLE platform.channel_inbox (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    external_message_id TEXT NOT NULL CHECK (external_message_id <> ''),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('ADMITTED', 'REJECTED')),
    message_type TEXT NOT NULL CHECK (message_type IN ('text', 'image', 'file', 'mixed', 'card', 'event', 'unsupported')),
    reject_reason TEXT,
    provider_reply_target_envelope JSONB,
    reply_target_expires_at TIMESTAMPTZ,
    provider_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, external_message_id),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    CHECK (
        (status = 'ADMITTED' AND reject_reason IS NULL)
        OR (
            status = 'REJECTED'
            AND reject_reason IN ('UNSUPPORTED_MESSAGE_TYPE', 'ATTACHMENT_REJECTED')
        )
    ),
    CHECK (
        (provider_reply_target_envelope IS NULL AND reply_target_expires_at IS NULL)
        OR (provider_reply_target_envelope IS NOT NULL AND reply_target_expires_at IS NOT NULL)
    ),
    CHECK (
        provider_reply_target_envelope IS NULL
        OR jsonb_typeof(provider_reply_target_envelope) = 'object'
    )
);

CREATE INDEX channel_inbox_request_idx
    ON platform.channel_inbox (tenant_id, app_id, request_id);

CREATE TABLE platform.channel_inbox_rejection_audit (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    external_message_id TEXT NOT NULL,
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    event_type TEXT NOT NULL CHECK (event_type = 'CHANNEL_MESSAGE_REJECTED'),
    reject_reason TEXT NOT NULL CHECK (reject_reason IN ('UNSUPPORTED_MESSAGE_TYPE', 'ATTACHMENT_REJECTED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, binding_id, external_message_id, event_type),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);
