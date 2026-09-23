SELECT TO_JSON_STRING(STRUCT(actor_name AS actorName, actor_id AS actorId,
    COUNT(*) AS admitted, AVG(queue_wait_ms) AS averageMs, MAX(queue_wait_ms) AS maxMs
)) AS result FROM events
WHERE queue_wait_ms IS NOT NULL AND outcome <> 'rerouted' AND (@actor_name IS NULL OR actor_name = @actor_name)
GROUP BY actor_name, actor_id
ORDER BY COUNT(*) DESC, actor_name, actor_id LIMIT 500
