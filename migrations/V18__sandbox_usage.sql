ALTER TABLE durable_actors_deployment ADD COLUMN billing_account_id TEXT;
ALTER TABLE durable_actors_spares ADD COLUMN usage_assignment JSONB;
ALTER TABLE durable_actors_spares DROP CONSTRAINT durable_actors_spares_status_check;
ALTER TABLE durable_actors_spares ADD CONSTRAINT durable_actors_spares_status_check
    CHECK (status IN ('starting', 'ready', 'claimed', 'active', 'retiring', 'stopping'));

CREATE TABLE durable_actors_usage_outbox (
    id TEXT PRIMARY KEY,
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX durable_actors_usage_pending ON durable_actors_usage_outbox (created_at);
