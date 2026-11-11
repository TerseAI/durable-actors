WITH sandboxes AS (
    SELECT sandbox_id
    FROM actor_sandboxes FINAL
    WHERE project_id = {project:String} AND actor_name = {actor_name:String}
      AND actor_id = {actor_id:String} AND assigned_at_ms <= {to:UInt64}
)
SELECT killed_at_ms, reason, message
FROM sandbox_kills FINAL
WHERE sandbox_id IN (SELECT sandbox_id FROM sandboxes)
  AND killed_at_ms BETWEEN {from:UInt64} AND {to:UInt64}
ORDER BY killed_at_ms
LIMIT {limit:UInt64}
