/* inventory snapshot */,
    actor_types AS (
        SELECT actor_name, sum(live) AS live, sum(dormant) AS dormant, sum(unknown) AS unknown
        FROM (
            SELECT actor_name, countIf(status = 'live') AS live, countIf(status = 'dormant') AS dormant,
                countIf(status = 'unknown') AS unknown
            FROM snapshot
            GROUP BY actor_name
            UNION ALL
            SELECT arrayJoin(JSONExtract({deployed:String}, 'Array(String)')), toUInt64(0), toUInt64(0), toUInt64(0)
        )
        GROUP BY actor_name
    )
SELECT counts, actors
FROM (
    SELECT tuple(sum(live), sum(dormant), count(), max(live + dormant + unknown)) AS counts
    FROM actor_types
) AS summary
CROSS JOIN (
    SELECT groupArray(tuple(actor_name, live, dormant, unknown)) AS actors
    FROM (
        SELECT actor_name, live, dormant, unknown FROM actor_types
        ORDER BY live + dormant + unknown DESC, actor_name ASC
        LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    )
) AS page
