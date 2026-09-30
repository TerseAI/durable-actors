ALTER TABLE durable_actors_usage_sessions ADD COLUMN observed_ms BIGINT;
UPDATE durable_actors_usage_sessions SET observed_ms = checkpoint_ms;
ALTER TABLE durable_actors_usage_sessions ALTER COLUMN observed_ms SET NOT NULL;
ALTER TABLE durable_actors_usage_sessions ALTER COLUMN observed_ms SET DEFAULT 0;
CREATE INDEX durable_actors_usage_unpublished ON durable_actors_usage_sessions (checkpoint_ms, session_id) WHERE observed_ms > checkpoint_ms;
ALTER TABLE durable_actors_deployment ADD COLUMN billing_account_id TEXT;
