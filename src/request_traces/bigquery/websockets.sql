, sessions AS (
    SELECT connection_id, actor_name, actor_id, MIN(host_id) AS host_id,
        MIN(IF(operation = 'onConnect', UNIX_MILLIS(started_at), NULL)) AS opened_at_ms,
        MAX(IF(operation = 'onDisconnect', UNIX_MILLIS(started_at), NULL)) AS closed_at_ms,
        MAX(UNIX_MILLIS(started_at)) AS last_seen_ms,
        MAX(IF(operation = 'onConnect', TO_JSON_STRING(metadata), NULL)) AS metadata,
        COUNTIF(operation = 'onMessage') AS messages,
        COUNTIF(outcome NOT IN ('completed', 'rerouted')) AS failures
    FROM events WHERE kind = 'websocket' AND connection_id IS NOT NULL
    GROUP BY connection_id, actor_name, actor_id
    HAVING MAX(UNIX_MILLIS(started_at)) >= @from_ms AND MIN(UNIX_MILLIS(started_at)) <= @to_ms
)
SELECT TO_JSON_STRING(STRUCT(connection_id AS connectionId, actor_name AS actorName, actor_id AS actorId,
    host_id AS hostId, opened_at_ms AS openedAtMs, closed_at_ms AS closedAtMs, last_seen_ms AS lastSeenMs,
    messages, failures, PARSE_JSON(metadata) AS metadata
)) AS result FROM sessions ORDER BY COALESCE(opened_at_ms, last_seen_ms) DESC, connection_id LIMIT 500
