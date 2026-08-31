CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE platform.channel_binding
    ADD COLUMN public_route_id TEXT DEFAULT (
        'r_' || translate(rtrim(encode(gen_random_bytes(16), 'base64'), '='), '+/', '-_')
    ),
    ADD COLUMN binding_revision BIGINT DEFAULT 1;

UPDATE platform.channel_binding
SET public_route_id = 'r_' || translate(rtrim(encode(gen_random_bytes(16), 'base64'), '='), '+/', '-_'),
    binding_revision = 1
WHERE public_route_id IS NULL OR binding_revision IS NULL;
