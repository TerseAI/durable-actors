ALTER TABLE durable_actors_deployment ADD COLUMN billing_account_id TEXT;
ALTER TABLE durable_actors_spares ADD COLUMN usage_assignment JSONB;

CREATE TABLE durable_actors_usage_outbox (
    id TEXT PRIMARY KEY,
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX durable_actors_usage_pending ON durable_actors_usage_outbox (created_at);
