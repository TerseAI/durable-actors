WITH inserted AS (
    INSERT INTO durable_actors_traces (
        project_id, position, event_id, event, started_at_ms, actor_name, actor_id,
        outcome, duration_ms, queue_wait_ms, kind, operation, connection_id, host_id
    )
    SELECT $1, $2::bigint + ordinal, event_id, event, started_at_ms, actor_name, actor_id,
        outcome, duration_ms, queue_wait_ms, kind, operation, connection_id, host_id
    FROM unnest(
        $3::text[], $4::text[], $5::bigint[], $6::text[], $7::text[], $8::text[],
        $9::double precision[], $10::double precision[], $11::text[], $12::text[],
        $13::text[], $14::text[]
    ) WITH ORDINALITY AS batch(
        event_id, event, started_at_ms, actor_name, actor_id, outcome, duration_ms,
        queue_wait_ms, kind, operation, connection_id, host_id, ordinal
    )
    ON CONFLICT (event_id) DO NOTHING
    RETURNING position
)
SELECT COUNT(*), MAX(position) FROM inserted
