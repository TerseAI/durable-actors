CREATE TABLE durable_actors_usage_sessions (
    session_id TEXT PRIMARY KEY,
    assignment JSONB NOT NULL,
    checkpoint_ms BIGINT NOT NULL,
    stopped BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE durable_actors_usage_outbox (
    id TEXT PRIMARY KEY,
    event JSONB NOT NULL,
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX durable_actors_usage_pending ON durable_actors_usage_outbox (created_at) WHERE delivered_at IS NULL;

CREATE INDEX durable_actors_usage_active ON durable_actors_usage_sessions (session_id) WHERE NOT stopped;
