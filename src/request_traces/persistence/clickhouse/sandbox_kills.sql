WITH sandboxes AS (
    SELECT sandbox_id, memory_limit_bytes
    FROM actor_sandboxes FINAL
    WHERE project_id = {project:String} AND actor_name = {actor_name:String}
      AND actor_id = {actor_id:String} AND assigned_at_ms <= {to:UInt64}
)
SELECT k.killed_at_ms AS killed_at_ms, k.reason AS reason, k.message AS message,
       s.memory_limit_bytes AS memory_limit_bytes
FROM (
    SELECT sandbox_id, killed_at_ms, reason, message
    FROM sandbox_kills FINAL
    WHERE sandbox_id IN (SELECT sandbox_id FROM sandboxes)
      AND killed_at_ms BETWEEN {from:UInt64} AND {to:UInt64}
) AS k
INNER JOIN sandboxes AS s USING sandbox_id
ORDER BY killed_at_ms
LIMIT {limit:UInt64}
