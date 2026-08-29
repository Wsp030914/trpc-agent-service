CREATE TABLE platform.knowledge_index_job (
    job_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL,
    document_id TEXT NOT NULL,
    document_version INTEGER NOT NULL CHECK (document_version >= 0),
    index_generation TEXT NOT NULL CHECK (index_generation <> ''),
    config_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    run_token TEXT,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, knowledge_base_id, document_id, document_version)
        REFERENCES platform.knowledge_document (tenant_id, app_id, knowledge_base_id, document_id, version),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version)
);
CREATE UNIQUE INDEX knowledge_index_job_active_document_idx
    ON platform.knowledge_index_job (
        tenant_id, app_id, knowledge_base_id, document_id, document_version, index_generation
    ) WHERE status IN ('PENDING', 'RUNNING');
CREATE INDEX knowledge_index_job_claim_idx
    ON platform.knowledge_index_job (status, next_attempt_at, job_id)
    WHERE status IN ('PENDING', 'RUNNING');
