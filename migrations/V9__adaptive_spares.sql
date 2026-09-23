CREATE TABLE durable_actors_pool_events (
    pool_key TEXT NOT NULL,
    event_kind TEXT NOT NULL CHECK (event_kind IN ('acquire', 'ready')),
    identity TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    startup_ms BIGINT CHECK (startup_ms > 0),
    PRIMARY KEY (pool_key, event_kind, identity)
);
CREATE INDEX durable_actors_pool_events_recent ON durable_actors_pool_events (pool_key, event_kind, created_at);
CREATE INDEX durable_actors_pool_events_expiration ON durable_actors_pool_events (created_at);

CREATE TABLE durable_actors_pool_targets (
    pool_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('actor', 'replica')),
    target INTEGER NOT NULL CHECK (target BETWEEN 0 AND 128),
    shrink_at_ms BIGINT,
    failures INTEGER NOT NULL DEFAULT 0 CHECK (failures BETWEEN 0 AND 6),
    retry_after TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX durable_actors_spares_unassigned ON durable_actors_spares (pool_key, status, expires_at)
    WHERE status IN ('ready', 'starting') OR (status = 'retiring' AND host_id IS NULL AND handle IS NOT NULL);
