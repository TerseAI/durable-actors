CREATE TABLE IF NOT EXISTS actor_logs (
    project_id String,
    event_id String,
    timestamp_ms UInt64,
    actor_name LowCardinality(String),
    actor_id String,
    host_id String,
    session_id String,
    request_id Nullable(String),
    stream LowCardinality(String),
    level LowCardinality(String),
    message String,
    fields_json String,
    event_json String,
    received_at_ms UInt64,
    received_at DateTime DEFAULT toDateTime(received_at_ms / 1000)
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, timestamp_ms, event_id)
PRIMARY KEY (project_id, timestamp_ms)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
CREATE TABLE IF NOT EXISTS actor_log_status (
    project_id String,
    report_id String,
    dropped UInt64,
    received_at_ms UInt64,
    received_at DateTime DEFAULT toDateTime(received_at_ms / 1000)
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, report_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
ALTER TABLE actor_logs MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
ALTER TABLE actor_log_status MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
