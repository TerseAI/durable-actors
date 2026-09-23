WITH scoped AS (
    SELECT actor_name, outcome, json_extract(event, '$.durationMs') AS duration_ms, json_extract(event, '$.queueWaitMs') AS queue_wait_ms FROM traces
    WHERE (?3 IS NULL OR project_id = ?3) AND (?1 IS NULL OR started_at_ms >= ?1) AND (?2 IS NULL OR started_at_ms <= ?2)
), events AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms FROM scoped
    UNION ALL
    SELECT '' AS actor_name, outcome, duration_ms, queue_wait_ms FROM scoped
), attempts AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms,
        ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY duration_ms) AS duration_rank,
        COUNT(*) OVER (PARTITION BY actor_name) AS attempts,
        ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY queue_wait_ms IS NULL, queue_wait_ms) AS queue_rank,
        COUNT(queue_wait_ms) OVER (PARTITION BY actor_name) AS queued
    FROM events WHERE outcome <> 'rerouted'
), summary AS (
    SELECT actor_name, MAX(attempts) AS attempts, SUM(outcome = 'completed') AS completed,
        MAX(CASE WHEN duration_rank = MAX(1, (attempts * 95 + 99) / 100) THEN duration_ms END) AS p95_duration_ms,
        MAX(CASE WHEN queued > 0 AND queue_rank = MAX(1, (queued * 95 + 99) / 100) THEN queue_wait_ms END) AS p95_queue_wait_ms
    FROM attempts GROUP BY actor_name
)
SELECT e.actor_name, COUNT(*) AS total, COALESCE(s.attempts, 0) AS attempts, COALESCE(s.completed, 0) AS completed, s.p95_duration_ms, s.p95_queue_wait_ms
FROM events e LEFT JOIN summary s ON s.actor_name = e.actor_name
GROUP BY e.actor_name
ORDER BY e.actor_name
LIMIT 500
