SELECT GatewayId AS gateway_id, toUInt64(max(toUnixTimestamp64Milli(Timestamp))) AS alive_at_ms
FROM platform_events
WHERE ProjectId = {project:String}
    AND EventName = 'durable_actors.gateway.alive'
    AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
GROUP BY GatewayId
