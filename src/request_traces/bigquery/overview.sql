, grouped AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms FROM events
    UNION ALL SELECT '' AS actor_name, outcome, duration_ms, queue_wait_ms FROM events
), attempts AS (
    SELECT actor_name, outcome, duration_ms, queue_wait_ms,
        ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY duration_ms) AS duration_rank,
        COUNT(*) OVER (PARTITION BY actor_name) AS attempts,
        ROW_NUMBER() OVER (PARTITION BY actor_name ORDER BY queue_wait_ms IS NULL, queue_wait_ms) AS queue_rank,
        COUNT(queue_wait_ms) OVER (PARTITION BY actor_name) AS queued
    FROM grouped WHERE outcome <> 'rerouted'
), summary AS (
    SELECT actor_name, MAX(attempts) AS attempts, COUNTIF(outcome = 'completed') AS completed,
        MAX(IF(duration_rank = CAST(CEIL(attempts * 0.95) AS INT64), duration_ms, NULL)) AS p95,
        MAX(IF(queued > 0 AND queue_rank = CAST(CEIL(queued * 0.95) AS INT64), queue_wait_ms, NULL)) AS queue_p95
    FROM attempts GROUP BY actor_name
)
SELECT TO_JSON_STRING(STRUCT(e.actor_name AS actorName, COUNT(*) AS count,
    100.0 * SAFE_DIVIDE(s.completed, s.attempts) AS success, s.p95, s.queue_p95 AS queueP95
)) AS result
FROM grouped e LEFT JOIN summary s ON s.actor_name = e.actor_name
GROUP BY e.actor_name, s.completed, s.attempts, s.p95, s.queue_p95
ORDER BY e.actor_name LIMIT 500
