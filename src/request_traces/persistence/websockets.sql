WITH events AS (
    SELECT *, json_extract(event, '$.hostId') AS host_id,
        json_extract(event, '$.connectionId') AS connection_id,
        json_extract(event, '$.kind') AS kind,
        json_extract(event, '$.operation') AS operation
    FROM traces
)
SELECT connection_id, actor_name, actor_id, MIN(host_id) AS host_id,
    MIN(CASE WHEN operation = 'onConnect' THEN started_at_ms END) AS opened_at_ms,
    MAX(CASE WHEN operation = 'onDisconnect' THEN started_at_ms END) AS closed_at_ms,
    MAX(started_at_ms) AS last_seen_ms,
    MAX(CASE WHEN operation = 'onConnect' THEN event END) AS connect_event,
    SUM(operation = 'onMessage') AS messages,
    SUM(outcome NOT IN ('completed', 'rerouted')) AS failures
FROM events
WHERE kind = 'websocket' AND connection_id IS NOT NULL
GROUP BY connection_id, actor_name, actor_id
HAVING (?1 IS NULL OR MAX(started_at_ms) >= ?1) AND (?2 IS NULL OR MIN(started_at_ms) <= ?2)
ORDER BY COALESCE(opened_at_ms, last_seen_ms) DESC, connection_id
LIMIT 500
