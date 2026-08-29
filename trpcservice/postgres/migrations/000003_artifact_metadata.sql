CREATE TABLE platform.artifact (
    artifact_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''),
    filename TEXT NOT NULL CHECK (filename <> ''),
    version INTEGER NOT NULL CHECK (version >= 0),
    object_key TEXT NOT NULL CHECK (object_key <> ''),
    mime_type TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    status TEXT NOT NULL CHECK (status IN ('AVAILABLE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, session_principal_id, session_id, filename, version),
    FOREIGN KEY (tenant_id, app_id, session_principal_id, session_id)
        REFERENCES platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
);
CREATE INDEX artifact_session_available_idx
    ON platform.artifact (tenant_id, app_id, session_principal_id, session_id, filename, version DESC)
    WHERE status = 'AVAILABLE';

CREATE TABLE platform.knowledge_document (
    tenant_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    document_id TEXT NOT NULL CHECK (document_id <> ''),
    version INTEGER NOT NULL CHECK (version >= 0),
    object_key TEXT NOT NULL CHECK (object_key <> ''),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'AVAILABLE', 'DELETED')),
    acl_policy JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (jsonb_typeof(acl_policy) = 'object'),
    index_generation TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, knowledge_base_id, document_id, version),
    FOREIGN KEY (tenant_id) REFERENCES platform.tenant (tenant_id)
);
CREATE INDEX knowledge_document_available_idx
    ON platform.knowledge_document (tenant_id, knowledge_base_id, document_id, version DESC)
    WHERE status = 'AVAILABLE';
