CREATE TABLE platform.channel_identity (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel <> ''),
    external_user_key_hash BYTEA NOT NULL CHECK (octet_length(external_user_key_hash) = 32),
    user_id TEXT NOT NULL CHECK (user_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    key_version TEXT NOT NULL CHECK (key_version <> ''),
    provider_target_envelope JSONB NOT NULL CHECK (jsonb_typeof(provider_target_envelope) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, user_id),
    UNIQUE (tenant_id, app_id, binding_id, external_user_key_hash),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);

CREATE INDEX channel_identity_scope_idx
    ON platform.channel_identity (tenant_id, app_id, binding_id, user_id);

CREATE TABLE platform.channel_conversation (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel <> ''),
    external_chat_key_hash BYTEA NOT NULL CHECK (octet_length(external_chat_key_hash) = 32),
    thread_key_hash BYTEA NOT NULL CHECK (octet_length(thread_key_hash) = 32),
    conversation_id TEXT NOT NULL CHECK (conversation_id <> ''),
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    scope TEXT NOT NULL CHECK (scope IN ('group', 'topic')),
    key_version TEXT NOT NULL CHECK (key_version <> ''),
    provider_target_envelope JSONB NOT NULL CHECK (jsonb_typeof(provider_target_envelope) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, conversation_id),
    UNIQUE (tenant_id, app_id, binding_id, external_chat_key_hash, thread_key_hash),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);

CREATE INDEX channel_conversation_scope_idx
    ON platform.channel_conversation (tenant_id, app_id, binding_id, conversation_id);

CREATE TABLE platform.channel_membership (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, conversation_id, user_id),
    FOREIGN KEY (tenant_id, app_id, conversation_id)
        REFERENCES platform.channel_conversation (tenant_id, app_id, conversation_id),
    FOREIGN KEY (tenant_id, app_id, user_id)
        REFERENCES platform.channel_identity (tenant_id, app_id, user_id)
);

CREATE INDEX channel_membership_user_idx
    ON platform.channel_membership (tenant_id, app_id, user_id);
