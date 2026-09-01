ALTER TABLE platform.execution
    ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;

ALTER TABLE platform.execution
    DROP CONSTRAINT IF EXISTS execution_status_check;

ALTER TABLE platform.execution
    ADD CONSTRAINT execution_status_check
    CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED'));

CREATE INDEX execution_cancel_requested_idx
    ON platform.execution (tenant_id, app_id, status, cancel_requested)
    WHERE cancel_requested = true;

CREATE TABLE platform.channel_recall_inbox (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    external_event_id TEXT NOT NULL CHECK (external_event_id <> ''),
    request_id TEXT,
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    status TEXT NOT NULL CHECK (status IN ('APPLIED', 'REJECTED')),
    reject_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, external_event_id),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    CHECK (
        (status = 'APPLIED' AND request_id IS NOT NULL AND request_id <> '' AND reject_reason IS NULL)
        OR (status = 'REJECTED' AND reject_reason IN ('REQUEST_NOT_FOUND', 'UNSUPPORTED_EVENT'))
    )
);

CREATE INDEX channel_recall_inbox_request_idx
    ON platform.channel_recall_inbox (tenant_id, app_id, request_id);
