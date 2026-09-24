SELECT connection_id, actor_name, actor_id, MIN(host_id) AS host_id,
    MIN(started_at_ms) FILTER (WHERE operation = 'onConnect') AS opened_at_ms,
    MAX(started_at_ms) FILTER (WHERE operation = 'onDisconnect') AS closed_at_ms,
    MAX(started_at_ms) AS last_seen_ms,
    MAX(event COLLATE "C") FILTER (WHERE operation = 'onConnect') AS connect_event,
    COUNT(*) FILTER (WHERE operation = 'onMessage') AS messages,
    COUNT(*) FILTER (WHERE outcome NOT IN ('completed', 'rerouted')) AS failures
FROM durable_actors_traces
WHERE project_id = $1 AND kind = 'websocket' AND connection_id IS NOT NULL
GROUP BY connection_id, actor_name, actor_id
HAVING ($2::bigint IS NULL OR MAX(started_at_ms) >= $2)
    AND ($3::bigint IS NULL OR MIN(started_at_ms) <= $3)
ORDER BY COALESCE(MIN(started_at_ms) FILTER (WHERE operation = 'onConnect'), MAX(started_at_ms)) DESC, connection_id
LIMIT 500
