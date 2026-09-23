ALTER TABLE durable_actors_deployment ADD COLUMN code_snapshot TEXT;

CREATE TABLE durable_actors_spares (
    name TEXT PRIMARY KEY,
    pool_key TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('starting', 'ready', 'claimed', 'active', 'retiring')),
    handle TEXT,
    host_id TEXT,
    code_revision TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX durable_actors_spares_available ON durable_actors_spares (pool_key, status, expires_at);
CREATE UNIQUE INDEX durable_actors_spares_host ON durable_actors_spares (host_id) WHERE host_id IS NOT NULL;
