WITH sandboxes AS (
    SELECT sandbox_id, assigned_at_ms, memory_limit_bytes
    FROM actor_sandboxes FINAL
    WHERE project_id = {project:String} AND actor_name = {actor_name:String}
      AND actor_id = {actor_id:String} AND assigned_at_ms <= {to:UInt64}
),
memory AS (
    SELECT intDiv(m.at_ms, {bucket:UInt64}) * {bucket:UInt64} AS bucket,
           toUInt64(max(m.bytes)) AS memory_bytes,
           max(s.memory_limit_bytes) AS memory_limit_bytes
    FROM (
        SELECT SandboxId AS sandbox_id, toUInt64(toUnixTimestamp(TimeUnix)) * 1000 AS at_ms,
               Value AS bytes
        FROM otel_metrics_gauge
        WHERE MetricName = 'k8s.pod.memory.working_set'
          AND SandboxId IN (SELECT sandbox_id FROM sandboxes)
          AND TimeUnix BETWEEN toDateTime(intDiv({from:UInt64}, 1000))
                           AND toDateTime(intDiv({to:UInt64}, 1000))
    ) AS m
    INNER JOIN sandboxes AS s USING sandbox_id
    WHERE m.at_ms >= s.assigned_at_ms
    GROUP BY bucket
),
-- CPU time is cumulative, so each sample's rate comes from the previous sample of the same pod,
-- read from a minute before the range so the first sample in it has one.
cpu AS (
    SELECT intDiv(c.at_ms, {bucket:UInt64}) * {bucket:UInt64} AS bucket,
           toNullable(toUInt64(greatest(avg(c.millicores), 0))) AS cpu_millicores
    FROM (
        SELECT sandbox_id, at_ms,
               lagInFrame(at_ms) OVER pod AS previous_ms,
               (seconds - lagInFrame(seconds) OVER pod) * 1000000
                   / (at_ms - lagInFrame(at_ms) OVER pod) AS millicores
        FROM (
            SELECT SandboxId AS sandbox_id, toUInt64(toUnixTimestamp(TimeUnix)) * 1000 AS at_ms,
                   Value AS seconds
            FROM otel_metrics_sum
            WHERE MetricName = 'k8s.pod.cpu.time'
              AND SandboxId IN (SELECT sandbox_id FROM sandboxes)
              AND TimeUnix BETWEEN toDateTime(intDiv(greatest({from:UInt64}, 60000) - 60000, 1000))
                               AND toDateTime(intDiv({to:UInt64}, 1000))
        )
        WINDOW pod AS (PARTITION BY sandbox_id ORDER BY at_ms ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
    ) AS c
    INNER JOIN sandboxes AS s USING sandbox_id
    WHERE c.previous_ms > 0 AND c.at_ms > c.previous_ms AND c.at_ms >= s.assigned_at_ms
      AND c.at_ms >= {from:UInt64}
    GROUP BY bucket
)
SELECT memory.bucket AS at_ms, cpu.cpu_millicores AS cpu_millicores,
       memory.memory_bytes AS memory_bytes, memory.memory_limit_bytes AS memory_limit_bytes
FROM memory
LEFT JOIN cpu USING bucket
ORDER BY at_ms
