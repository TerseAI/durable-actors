SELECT ConnectionId AS connection_id,
    any(ActorName) AS actor_name,
    any(ActorId) AS actor_id,
    argMinIf(GatewayId, Timestamp, EventName = 'durable_actors.socket.opened') AS gateway_id,
    toUInt64(minIf(toUnixTimestamp64Milli(Timestamp), EventName = 'durable_actors.socket.opened'))
        AS opened_at_ms,
    argMinIf(LogAttributes['durable_actors.metadata'], Timestamp,
        EventName = 'durable_actors.socket.opened') AS metadata,
    toUInt64(maxIf(toUnixTimestamp64Milli(Timestamp), EventName = 'durable_actors.socket.closed'))
        AS closed_at_ms,
    argMaxIf(LogAttributes['durable_actors.close_code'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS close_code,
    argMaxIf(LogAttributes['durable_actors.close_reason'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS close_reason,
    argMaxIf(LogAttributes['durable_actors.close_clean'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS close_clean,
    argMaxIf(LogAttributes['durable_actors.messages_in'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS messages_in,
    argMaxIf(LogAttributes['durable_actors.messages_out'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS messages_out,
    argMaxIf(LogAttributes['durable_actors.bytes_in'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS bytes_in,
    argMaxIf(LogAttributes['durable_actors.bytes_out'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS bytes_out,
    argMaxIf(LogAttributes['durable_actors.duration_ms'], Timestamp,
        EventName = 'durable_actors.socket.closed') AS duration_ms
FROM socket_events
WHERE ProjectId = {project:String}
    AND ConnectionId != ''
    AND ReceivedAt > now64(3) - toIntervalSecond({retention:UInt64})
GROUP BY ConnectionId
HAVING opened_at_ms > 0 AND opened_at_ms <= {to:UInt64}
    AND (NOT {open_only:Bool} OR closed_at_ms = 0)
ORDER BY opened_at_ms DESC, connection_id
LIMIT {limit:UInt64}
