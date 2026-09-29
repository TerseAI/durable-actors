WITH ready AS (
    UPDATE durable_actors_replication_groups
    SET state = 'ready', ever_ready = TRUE, updated_at = clock_timestamp()
    WHERE prefix = $1 AND state IN ('creating', 'ready')
    RETURNING prefix, state, ever_ready, checkpoint
)
SELECT g.state, g.ever_ready, g.checkpoint,
    COALESCE((SELECT json_agg(p.config::json ORDER BY p.slot)
        FROM durable_actors_replication_pods p WHERE p.group_prefix = g.prefix), '[]'::json)
FROM ready g
