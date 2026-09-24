SELECT position, event FROM durable_actors_traces
WHERE project_id = $1 AND position <= $2
    AND ($3::text IS NULL OR actor_name = $3)
    AND ($4::text IS NULL OR actor_id = $4)
    AND ($5::text IS NULL OR outcome = $5)
    AND ($6::bigint IS NULL OR started_at_ms >= $6)
    AND ($7::bigint IS NULL OR started_at_ms <= $7)
    AND ($8::bigint IS NULL OR (started_at_ms, position) < ($8, $9::bigint))
ORDER BY started_at_ms DESC, position DESC LIMIT $10
