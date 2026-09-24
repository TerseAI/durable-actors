WITH events AS (
    SELECT actor_name, actor_id, outcome, started_at_ms, json_extract(event, '$.queueWaitMs') AS queue_wait_ms FROM traces WHERE project_id = ?4
)
SELECT actor_name, actor_id, COUNT(*) AS admitted, AVG(queue_wait_ms), MAX(queue_wait_ms)
FROM events
WHERE queue_wait_ms IS NOT NULL AND outcome <> 'rerouted'
    AND (?1 IS NULL OR started_at_ms >= ?1) AND (?2 IS NULL OR started_at_ms <= ?2)
    AND (?3 IS NULL OR actor_name = ?3)
GROUP BY actor_name, actor_id
ORDER BY admitted DESC, actor_name, actor_id
LIMIT 500
