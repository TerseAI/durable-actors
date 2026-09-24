CREATE TABLE durable_actors_pool_backoffs (
    pool_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('actor', 'replica')),
    failures INTEGER NOT NULL DEFAULT 0 CHECK (failures BETWEEN 0 AND 6),
    retry_after TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX durable_actors_spares_unassigned ON durable_actors_spares (pool_key, status, expires_at)
    WHERE status IN ('ready', 'starting') OR (status = 'retiring' AND host_id IS NULL AND handle IS NOT NULL);
