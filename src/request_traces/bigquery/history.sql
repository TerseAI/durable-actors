SELECT TO_JSON_STRING(STRUCT(
    event_id AS eventId, project_id AS projectId, region, host_id AS hostId, session_id AS sessionId,
    request_id AS requestId, actor_name AS actorName, actor_id AS actorId, kind, operation,
    connection_id AS connectionId, UNIX_MILLIS(started_at) AS startedAtMs,
    duration_ms AS durationMs, queue_wait_ms AS queueWaitMs, outcome, metadata
)) AS result
FROM events
WHERE (@actor_name IS NULL OR actor_name = @actor_name) AND (@actor_id IS NULL OR actor_id = @actor_id)
  AND (@outcome IS NULL OR outcome = @outcome)
ORDER BY started_at DESC, event_id DESC
