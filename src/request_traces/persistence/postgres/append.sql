WITH input AS (
    SELECT payload, payload::jsonb AS event, ordinal
    FROM unnest($3::text[]) WITH ORDINALITY AS batch(payload, ordinal)
), inserted AS (
    INSERT INTO durable_actors_traces (
        project_id, position, event_id, event, started_at_ms, actor_name, actor_id,
        outcome, duration_ms, queue_wait_ms, kind, operation, connection_id, host_id
    )
    SELECT $1, $2::bigint + ordinal, event->>'eventId', payload,
        (event->>'startedAtMs')::bigint, event->>'actorName', event->>'actorId',
        event->>'outcome', (event->>'durationMs')::double precision,
        (event->>'queueWaitMs')::double precision, event->>'kind', event->>'operation',
        event->>'connectionId', event->>'hostId'
    FROM input
    ON CONFLICT (event_id) DO NOTHING
    RETURNING position
)
SELECT COUNT(*), MAX(position) FROM inserted
