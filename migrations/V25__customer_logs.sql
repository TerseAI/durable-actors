CREATE TABLE durable_actors_customer_logs (
    project_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    timestamp_ms BIGINT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_id TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    event JSONB NOT NULL,
    received_at_ms BIGINT NOT NULL,
    PRIMARY KEY(project_id, event_id)
);
CREATE INDEX durable_actors_customer_logs_history ON durable_actors_customer_logs(project_id, timestamp_ms DESC, event_id DESC);
CREATE INDEX durable_actors_customer_logs_retention ON durable_actors_customer_logs(received_at_ms);
CREATE TABLE durable_actors_customer_log_status (
    project_id TEXT NOT NULL,
    report_id TEXT NOT NULL,
    dropped BIGINT NOT NULL,
    received_at_ms BIGINT NOT NULL,
    PRIMARY KEY(project_id, report_id)
);
CREATE INDEX durable_actors_customer_log_status_retention ON durable_actors_customer_log_status(received_at_ms);
