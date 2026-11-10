SELECT intDiv(JSONExtractUInt(event_json, 'occurredAtMs'), {bucket:UInt64}) * {bucket:UInt64}
           AS at_ms,
       max(JSONExtractUInt(event_json, 'databaseBytes')) AS bytes
FROM actor_lifecycle
WHERE project_id = {project:String} AND actor_name = {actor_name:String}
  AND actor_id = {actor_id:String}
  AND JSONHas(event_json, 'databaseBytes')
  AND JSONExtractUInt(event_json, 'occurredAtMs') BETWEEN {from:UInt64} AND {to:UInt64}
GROUP BY at_ms
ORDER BY at_ms
