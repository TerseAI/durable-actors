SELECT event_json FROM actor_logs FINAL
WHERE project_id={project:String}
  AND received_at_ms>=toUnixTimestamp64Milli(now64())-{retention:UInt64}*1000
  AND ({actor_name:String}='' OR actor_name={actor_name:String})
  AND ({actor_id:String}='' OR actor_id={actor_id:String})
  AND ({request_id:String}='' OR request_id={request_id:String})
  AND ({level:String}='' OR level={level:String})
  AND ({search:String}='' OR position(message,{search:String})>0)
  AND timestamp_ms BETWEEN {from:UInt64} AND {to:UInt64}
  AND (NOT {has_cursor:Bool} OR (timestamp_ms,event_id)<({after_time:UInt64},{after_id:String}))
ORDER BY timestamp_ms DESC,event_id DESC
LIMIT {limit:UInt64}
