SELECT actor_name, actor_id, COUNT(*) AS admitted, AVG(queue_wait_ms), MAX(queue_wait_ms)
FROM durable_actors_traces
WHERE project_id = $1 AND queue_wait_ms IS NOT NULL AND outcome <> 'rerouted'
    AND ($2::bigint IS NULL OR started_at_ms >= $2)
    AND ($3::bigint IS NULL OR started_at_ms <= $3)
    AND ($4::text IS NULL OR actor_name = $4)
GROUP BY actor_name, actor_id
ORDER BY admitted DESC, actor_name, actor_id
LIMIT 500
