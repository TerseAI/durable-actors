CREATE TABLE IF NOT EXISTS actor_logs (
    project_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_id TEXT,
    level TEXT NOT NULL,
    message TEXT NOT NULL,
    event_json TEXT NOT NULL,
    received_at_ms INTEGER NOT NULL,
    PRIMARY KEY(project_id, event_id)
);
CREATE INDEX IF NOT EXISTS actor_logs_history ON actor_logs(project_id, timestamp_ms DESC, event_id DESC);
CREATE INDEX IF NOT EXISTS actor_logs_retention ON actor_logs(received_at_ms);
CREATE TABLE IF NOT EXISTS actor_log_status (
    project_id TEXT NOT NULL,
    report_id TEXT NOT NULL,
    dropped INTEGER NOT NULL,
    received_at_ms INTEGER NOT NULL,
    PRIMARY KEY(project_id, report_id)
);
