WITH sessions AS (
    SELECT connection_id, actor_name, actor_id,
        MIN(started_at_ms) FILTER (WHERE operation = 'onConnect') AS opened_at_ms,
        MAX(started_at_ms) FILTER (WHERE operation = 'onDisconnect') AS closed_at_ms,
        MAX(started_at_ms) AS last_seen_ms,
        COUNT(*) FILTER (WHERE operation = 'onMessage') AS messages,
        COUNT(*) FILTER (WHERE outcome NOT IN ('completed', 'rerouted')) AS failures
    FROM durable_actors_traces
    WHERE project_id = $1 AND kind = 'websocket' AND connection_id IS NOT NULL
    GROUP BY connection_id, actor_name, actor_id
    -- Filter session spans while preserving events outside the requested interval.
    HAVING ($2::bigint IS NULL OR MAX(started_at_ms) >= $2)
        AND ($3::bigint IS NULL OR MIN(started_at_ms) <= $3)
    ORDER BY COALESCE(MIN(started_at_ms) FILTER (WHERE operation = 'onConnect'), MAX(started_at_ms)) DESC, connection_id
    LIMIT 500
)
SELECT s.connection_id, s.actor_name, s.actor_id, first.host_id,
    s.opened_at_ms, s.closed_at_ms, s.last_seen_ms,
    CASE WHEN first.operation = 'onConnect' THEN first.event END AS connect_event,
    s.messages, s.failures
FROM sessions s
CROSS JOIN LATERAL (
    SELECT host_id, operation, event
    FROM durable_actors_traces
    WHERE project_id = $1 AND kind = 'websocket'
        AND connection_id = s.connection_id AND actor_name = s.actor_name AND actor_id = s.actor_id
    ORDER BY (operation = 'onConnect') DESC, started_at_ms, position
    LIMIT 1
) first
ORDER BY COALESCE(s.opened_at_ms, s.last_seen_ms) DESC, s.connection_id
