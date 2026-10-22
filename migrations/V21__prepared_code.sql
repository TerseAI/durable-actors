CREATE TABLE durable_actors_prepared_code (
    cache_key TEXT PRIMARY KEY,
    image_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX durable_actors_prepared_code_expiry ON durable_actors_prepared_code (expires_at);

CREATE TABLE durable_actors_preparing_pools (
    image_ref TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);
