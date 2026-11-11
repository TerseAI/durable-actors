-- Heartbeats carry no project, so they sit under the empty one.
SELECT GatewayId AS gateway_id, toUInt64(max(toUnixTimestamp64Milli(Timestamp))) AS alive_at_ms
FROM socket_events
WHERE ProjectId = ''
    AND EventName = 'durable_actors.gateway.alive'
    AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
GROUP BY GatewayId
