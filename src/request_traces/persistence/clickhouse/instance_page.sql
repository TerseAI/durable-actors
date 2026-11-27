/* inventory snapshot */,
    (({status:String} = '' OR status = {status:String})
        AND ({search:String} = '' OR positionCaseInsensitiveUTF8(actor_id, {search:String}) > 0)) AS matched
SELECT counts, instances
FROM (
    SELECT tuple(countIf(status = 'live'), countIf(status = 'dormant'), countIf(status = 'unknown'), countIf(matched)) AS counts
    FROM snapshot
) AS summary
CROSS JOIN (
    SELECT groupArray(tuple(actor_id, event_json, socket_count)) AS instances
    FROM (
        SELECT actor_id, event_json, socket_count FROM snapshot
        WHERE matched
        ORDER BY /* page order */
        LIMIT {limit:UInt32} OFFSET {offset:UInt32}
    )
) AS page
