CREATE TABLE platform.data_migration (
    migration_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    source_config_version TEXT NOT NULL,
    target_config_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'DRAINING', 'COPYING', 'VERIFYING', 'SUCCEEDED', 'FAILED')),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    run_token TEXT,
    drain_deadline TIMESTAMPTZ,
    progress JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(progress) = 'object'),
    validation_result JSONB NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(validation_result) = 'object'),
    failure_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((lease_owner IS NULL AND lease_until IS NULL AND run_token IS NULL) OR
           (lease_owner IS NOT NULL AND lease_until IS NOT NULL AND run_token IS NOT NULL)),
    CHECK (source_config_version <> target_config_version),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);
CREATE UNIQUE INDEX data_migration_active_scope_idx
    ON platform.data_migration (tenant_id, app_id)
    WHERE status IN ('PENDING', 'DRAINING', 'COPYING', 'VERIFYING');
CREATE INDEX data_migration_admission_gate_idx
    ON platform.data_migration (tenant_id, app_id, status);
