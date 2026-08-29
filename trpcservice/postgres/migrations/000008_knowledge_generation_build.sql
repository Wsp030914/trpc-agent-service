CREATE TABLE platform.knowledge_generation_build (
    build_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    config_version TEXT NOT NULL,
    index_generation TEXT NOT NULL CHECK (index_generation <> ''),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'SUCCEEDED', 'FAILED')),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, config_version),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version)
);
CREATE INDEX knowledge_generation_build_pending_idx
    ON platform.knowledge_generation_build (tenant_id, app_id, build_id)
    WHERE status = 'PENDING';

ALTER TABLE platform.knowledge_index_job
    ADD COLUMN build_id TEXT NOT NULL DEFAULT '';
UPDATE platform.knowledge_index_job
SET build_id = config_version
WHERE build_id = '';
ALTER TABLE platform.knowledge_index_job
    ADD CONSTRAINT knowledge_index_job_build_id_check CHECK (build_id <> '');
ALTER TABLE platform.knowledge_index_job
    DROP CONSTRAINT knowledge_index_job_status_check;
ALTER TABLE platform.knowledge_index_job
    ADD CONSTRAINT knowledge_index_job_status_check
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED'));
DROP INDEX platform.knowledge_index_job_active_document_idx;
CREATE UNIQUE INDEX knowledge_index_job_build_document_idx
    ON platform.knowledge_index_job (
        tenant_id, app_id, knowledge_base_id, document_id, document_version,
        index_generation, config_version, build_id
    );
CREATE INDEX knowledge_index_job_pending_build_idx
    ON platform.knowledge_index_job (build_id, status);
