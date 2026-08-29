ALTER TABLE platform.app_config_version
    ADD COLUMN knowledge_base_ids JSONB NOT NULL DEFAULT '[]'::JSONB
    CHECK (jsonb_typeof(knowledge_base_ids) = 'array');

CREATE TABLE platform.knowledge_base (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, knowledge_base_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

ALTER TABLE platform.knowledge_document
    ADD COLUMN app_id TEXT NOT NULL DEFAULT '';
ALTER TABLE platform.knowledge_document
    ADD COLUMN content_sha256 BYTEA NOT NULL DEFAULT '\\x'::bytea;
ALTER TABLE platform.knowledge_document
    ADD COLUMN mime_type TEXT NOT NULL DEFAULT '';
ALTER TABLE platform.knowledge_document
    DROP CONSTRAINT knowledge_document_pkey;
ALTER TABLE platform.knowledge_document
    ADD PRIMARY KEY (tenant_id, app_id, knowledge_base_id, document_id, version);
ALTER TABLE platform.knowledge_document
    ADD CONSTRAINT knowledge_document_app_fk
    FOREIGN KEY (tenant_id, app_id, knowledge_base_id)
    REFERENCES platform.knowledge_base (tenant_id, app_id, knowledge_base_id)
    NOT VALID;
CREATE INDEX knowledge_document_scope_available_idx
    ON platform.knowledge_document (tenant_id, app_id, knowledge_base_id, document_id, version DESC)
    WHERE status = 'AVAILABLE';

CREATE TABLE platform.knowledge_chunk (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    document_id TEXT NOT NULL CHECK (document_id <> ''),
    document_version INTEGER NOT NULL CHECK (document_version >= 0),
    index_generation TEXT NOT NULL CHECK (index_generation <> ''),
    chunk_id TEXT NOT NULL CHECK (chunk_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'AVAILABLE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        tenant_id,
        app_id,
        knowledge_base_id,
        document_id,
        document_version,
        index_generation,
        chunk_id
    ),
    FOREIGN KEY (tenant_id, app_id, knowledge_base_id, document_id, document_version)
        REFERENCES platform.knowledge_document (tenant_id, app_id, knowledge_base_id, document_id, version)
);
CREATE INDEX knowledge_chunk_available_idx
    ON platform.knowledge_chunk (
        tenant_id,
        app_id,
        knowledge_base_id,
        document_id,
        document_version,
        index_generation,
        chunk_id
    )
    WHERE status = 'AVAILABLE';
