ALTER TABLE platform.reply_outbox
    ADD COLUMN IF NOT EXISTS binding_revision BIGINT NOT NULL DEFAULT 1;

ALTER TABLE platform.reply_outbox
    ADD CONSTRAINT reply_outbox_binding_revision_positive
    CHECK (binding_revision > 0);
