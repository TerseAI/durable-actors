WITH events AS (
    SELECT * FROM `{{table}}`
    WHERE project_id = @project_id AND environment = @environment AND schema_version = 1
      AND started_at >= TIMESTAMP_MILLIS(@scan_from_ms) AND started_at <= TIMESTAMP_MILLIS(@scan_to_ms)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id, event_id ORDER BY received_at, publish_time, message_id) = 1
)
