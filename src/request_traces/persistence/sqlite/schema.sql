CREATE TABLE traces (
    event_id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    started_at_ms INTEGER NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    event TEXT NOT NULL
);
CREATE INDEX traces_project_time ON traces(project_id, started_at_ms DESC, event_id DESC);

CREATE TABLE actor_logs (
    position INTEGER PRIMARY KEY AUTOINCREMENT,
    log_id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    request_id TEXT,
    timestamp_ms INTEGER NOT NULL,
    log TEXT NOT NULL
);
CREATE INDEX actor_logs_time ON actor_logs(project_id, timestamp_ms DESC, log_id DESC);

-- Platform events as the collector stores them, keeping the latest of each: one row per
-- connection open and close, per gateway heartbeat, and per deployment event.
CREATE TABLE platform_events (
    project_id TEXT NOT NULL,
    name TEXT NOT NULL,
    gateway_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    at_ms INTEGER NOT NULL,
    attributes TEXT NOT NULL,
    body TEXT,
    PRIMARY KEY (project_id, name, gateway_id, actor_name, actor_id, connection_id)
);
