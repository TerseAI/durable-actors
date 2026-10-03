CREATE TABLE durable_actors_alarms (
    generation TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    region TEXT NOT NULL,
    deadline BIGINT NOT NULL,
    available_at BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    token TEXT,
    lease_until BIGINT NOT NULL DEFAULT 0
);
CREATE INDEX durable_actors_alarms_due ON durable_actors_alarms(available_at, lease_until);
