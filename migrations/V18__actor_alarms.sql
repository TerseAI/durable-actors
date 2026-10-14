CREATE TABLE durable_actors_alarm (
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    state_version BIGINT NOT NULL,
    timestamp_ms BIGINT,
    PRIMARY KEY (project_id, actor_name, actor_id)
);

CREATE INDEX durable_actors_alarm_due
ON durable_actors_alarm (timestamp_ms)
WHERE timestamp_ms IS NOT NULL;
