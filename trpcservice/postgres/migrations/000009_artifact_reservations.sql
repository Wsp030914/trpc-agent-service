ALTER TABLE platform.artifact
    DROP CONSTRAINT IF EXISTS artifact_status_check;
ALTER TABLE platform.artifact
    ADD CONSTRAINT artifact_status_check
        CHECK (status IN ('PENDING', 'AVAILABLE', 'DELETED'));
ALTER TABLE platform.artifact
    DROP CONSTRAINT IF EXISTS artifact_object_key_check;
ALTER TABLE platform.artifact
    ADD CONSTRAINT artifact_object_key_check
        CHECK (status = 'PENDING' OR object_key <> '');

ALTER TABLE platform.artifact_cleanup
    DROP CONSTRAINT IF EXISTS artifact_cleanup_status_check;
ALTER TABLE platform.artifact_cleanup
    ADD CONSTRAINT artifact_cleanup_status_check
        CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED'));
UPDATE platform.artifact_cleanup
SET status = 'FAILED',
    last_error = CASE WHEN last_error = ''
        THEN 'legacy all-version cleanup requires manual reconciliation'
        ELSE last_error || '; legacy all-version cleanup requires manual reconciliation'
    END,
    updated_at = clock_timestamp()
WHERE object_key = '' OR version < 0;
ALTER TABLE platform.artifact_cleanup
    ADD CONSTRAINT artifact_cleanup_exact_target_check
        CHECK (status = 'FAILED' OR (object_key <> '' AND version >= 0));
DROP INDEX platform.artifact_cleanup_active_scope_idx;
CREATE UNIQUE INDEX artifact_cleanup_active_object_idx
    ON platform.artifact_cleanup (
        tenant_id, app_id, config_version, session_principal_id, session_id, object_key, version
    ) WHERE status IN ('PENDING', 'RUNNING');
