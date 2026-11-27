WITH
    latest AS (
        SELECT actor_name, actor_id, argMaxMerge(event) AS event_json, toUInt64(0) AS socket_count
        FROM actor_inventory
        WHERE project_id = {project:String}
            AND ({actor_name:String} = '' OR actor_name = {actor_name:String})
            AND ({actor_id:String} = '' OR actor_id = {actor_id:String})
        GROUP BY actor_name, actor_id
    ),
    actors_and_sockets AS (
        SELECT actor_name, actor_id, max(event_json) AS event_json, sum(socket_count) AS socket_count
        FROM (
            SELECT actor_name, actor_id, event_json, socket_count FROM latest
            UNION ALL
            SELECT room.1, room.2, '', room.3
            FROM (SELECT arrayJoin(JSONExtract({sockets:String}, 'Array(Tuple(String, String, UInt64))')) AS room)
        )
        GROUP BY actor_name, actor_id
    ),
    snapshot AS (
        SELECT actor_name, actor_id, event_json, socket_count,
            multiIf(event_json = '' OR JSONExtractString(event_json, 'kind') = 'stopped', 'dormant',
                JSONExtractUInt(event_json, 'receivedAtMs') + {stale:UInt64} < {now:UInt64}, 'unknown',
                JSONExtractRaw(event_json, 'resident') = 'true', 'live',
                JSONExtractRaw(event_json, 'resident') = 'false', 'dormant',
                'unknown') AS status,
            JSONExtract(event_json, 'lastActiveAtMs', 'Nullable(UInt64)') AS last_active,
            JSONExtract(event_json, 'databaseBytes', 'Nullable(UInt64)') AS disk
        FROM actors_and_sockets
    )
