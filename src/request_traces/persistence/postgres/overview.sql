SELECT actor_name, COUNT(*) AS total,
    COUNT(*) FILTER (WHERE outcome <> 'rerouted') AS attempts,
    COUNT(*) FILTER (WHERE outcome = 'completed') AS completed,
    percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms) FILTER (WHERE outcome <> 'rerouted'),
    percentile_disc(0.95) WITHIN GROUP (ORDER BY queue_wait_ms) FILTER (WHERE outcome <> 'rerouted'),
    GROUPING(actor_name) = 1 AS is_total
FROM durable_actors_traces
WHERE project_id = $1
    AND ($2::bigint IS NULL OR started_at_ms >= $2)
    AND ($3::bigint IS NULL OR started_at_ms <= $3)
GROUP BY GROUPING SETS ((actor_name), ())
ORDER BY is_total DESC, actor_name
LIMIT 500
