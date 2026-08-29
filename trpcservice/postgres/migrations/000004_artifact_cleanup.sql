CREATE TABLE platform.artifact_cleanup (
    cleanup_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    config_version TEXT NOT NULL,
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''),
    filename TEXT NOT NULL CHECK (filename <> ''),
    object_key TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    run_token TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version),
    FOREIGN KEY (tenant_id, app_id, session_principal_id, session_id)
        REFERENCES platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
);
CREATE UNIQUE INDEX artifact_cleanup_active_scope_idx
    ON platform.artifact_cleanup (
        tenant_id, app_id, config_version, session_principal_id, session_id, filename
    ) WHERE status IN ('PENDING', 'RUNNING');
CREATE INDEX artifact_cleanup_claim_idx
    ON platform.artifact_cleanup (status, next_attempt_at, cleanup_id)
    WHERE status IN ('PENDING', 'RUNNING');
