WITH events AS (
    SELECT *, json_extract(event, '$.hostId') AS host_id,
        json_extract(event, '$.connectionId') AS connection_id,
        json_extract(event, '$.kind') AS kind,
        json_extract(event, '$.operation') AS operation
    FROM traces WHERE project_id = ?3
), ranked AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY connection_id, actor_name, actor_id
        ORDER BY operation = 'onConnect' DESC, started_at_ms, position
    ) AS session_rank
    FROM events WHERE kind = 'websocket' AND connection_id IS NOT NULL
)
SELECT connection_id, actor_name, actor_id,
    MAX(CASE WHEN session_rank = 1 THEN host_id END) AS host_id,
    MIN(CASE WHEN operation = 'onConnect' THEN started_at_ms END) AS opened_at_ms,
    MAX(CASE WHEN operation = 'onDisconnect' THEN started_at_ms END) AS closed_at_ms,
    MAX(started_at_ms) AS last_seen_ms,
    MAX(CASE WHEN session_rank = 1 AND operation = 'onConnect' THEN event END) AS connect_event,
    SUM(operation = 'onMessage') AS messages,
    SUM(outcome NOT IN ('completed', 'rerouted')) AS failures
FROM ranked
GROUP BY connection_id, actor_name, actor_id
HAVING (?1 IS NULL OR MAX(started_at_ms) >= ?1) AND (?2 IS NULL OR MIN(started_at_ms) <= ?2)
ORDER BY COALESCE(opened_at_ms, last_seen_ms) DESC, connection_id
LIMIT 500
