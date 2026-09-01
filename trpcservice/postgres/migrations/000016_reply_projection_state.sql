CREATE TABLE platform.reply_projection_state (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    last_event_seq BIGINT NOT NULL DEFAULT 0 CHECK (last_event_seq >= 0),
    content TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, request_id),
    FOREIGN KEY (tenant_id, app_id, request_id)
        REFERENCES platform.execution (tenant_id, app_id, request_id),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);
