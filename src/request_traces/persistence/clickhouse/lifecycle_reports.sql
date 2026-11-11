-- Hosts report the database size only once it is known, and never 0, so 0 means none.
SELECT intDiv(JSONExtractUInt(event_json, 'occurredAtMs'), {bucket:UInt64}) * {bucket:UInt64}
           AS at_ms,
       maxIf(JSONExtractUInt(event_json, 'databaseBytes'), JSONHas(event_json, 'databaseBytes'))
           AS database_bytes,
       argMax(JSONExtractRaw(event_json, 'resources'), JSONExtractUInt(event_json, 'occurredAtMs'))
           AS resources
FROM actor_lifecycle
WHERE project_id = {project:String} AND actor_name = {actor_name:String}
  AND actor_id = {actor_id:String}
  AND JSONExtractUInt(event_json, 'occurredAtMs') BETWEEN {from:UInt64} AND {to:UInt64}
GROUP BY at_ms
ORDER BY at_ms
