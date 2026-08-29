ALTER TABLE platform.artifact_cleanup
    ADD COLUMN version INTEGER NOT NULL DEFAULT -1 CHECK (version >= -1);
