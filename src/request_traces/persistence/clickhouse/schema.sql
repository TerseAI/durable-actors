-- Columns match the OpenTelemetry Collector ClickHouse exporter (v0.163.0), which inserts by name.
-- Tenant columns come from resource attributes the collector stamps from verified host claims.
-- Partitioning and retention use the server's insert time. Partitioning by host timestamps would
-- let one host's clock fail inserts that carry other projects' rows, and whole partitions expire cheaply.
CREATE TABLE IF NOT EXISTS otel_traces (
    Timestamp DateTime64(9) CODEC(Delta, ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    ParentSpanId String CODEC(ZSTD(1)),
    TraceState String CODEC(ZSTD(1)),
    SpanName LowCardinality(String) CODEC(ZSTD(1)),
    SpanKind LowCardinality(String) CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    SpanAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    Duration UInt64 CODEC(ZSTD(1)),
    StatusCode LowCardinality(String) CODEC(ZSTD(1)),
    StatusMessage String CODEC(ZSTD(1)),
    Events Nested (
        Timestamp DateTime64(9),
        Name LowCardinality(String),
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    Links Nested (
        TraceId String,
        SpanId String,
        TraceState String,
        Attributes Map(LowCardinality(String), String)
    ) CODEC(ZSTD(1)),
    ProjectId LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.project_id'],
    ActorName LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.actor_name'],
    ActorId String MATERIALIZED ResourceAttributes['durable_actors.actor_id'],
    HostId String MATERIALIZED ResourceAttributes['durable_actors.host_id'],
    SessionId String MATERIALIZED ResourceAttributes['durable_actors.session_id'],
    RequestId String MATERIALIZED SpanAttributes['durable_actors.request_id'],
    ReceivedAt DateTime64(3) DEFAULT now64(3),
    INDEX idx_request_id RequestId TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (ProjectId, ActorName, ActorId, Timestamp)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_logs (
    Timestamp DateTime64(9) CODEC(Delta(8), ZSTD(1)),
    TraceId String CODEC(ZSTD(1)),
    SpanId String CODEC(ZSTD(1)),
    TraceFlags UInt8,
    SeverityText LowCardinality(String) CODEC(ZSTD(1)),
    SeverityNumber UInt8,
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    Body String CODEC(ZSTD(1)),
    ResourceSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeSchemaUrl LowCardinality(String) CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion LowCardinality(String) CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    LogAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    EventName String CODEC(ZSTD(1)),
    ProjectId LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.project_id'],
    ActorName LowCardinality(String) MATERIALIZED ResourceAttributes['durable_actors.actor_name'],
    ActorId String MATERIALIZED ResourceAttributes['durable_actors.actor_id'],
    HostId String MATERIALIZED ResourceAttributes['durable_actors.host_id'],
    RequestId String MATERIALIZED LogAttributes['durable_actors.request_id'],
    LogId String MATERIALIZED LogAttributes['durable_actors.log_id'],
    ReceivedAt DateTime64(3) DEFAULT now64(3),
    INDEX idx_request_id RequestId TYPE bloom_filter(0.001) GRANULARITY 1
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (ProjectId, ActorName, ActorId, Timestamp)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

ALTER TABLE otel_traces MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;
ALTER TABLE otel_logs MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;

CREATE TABLE IF NOT EXISTS actor_lifecycle (
    project_id String,
    actor_name String,
    actor_id String,
    event_id String,
    owner_epoch UInt64,
    sequence UInt64,
    event_json String,
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, actor_name, actor_id, event_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE TABLE IF NOT EXISTS actor_inventory (
    project_id String,
    actor_name String,
    actor_id String,
    event AggregateFunction(argMax, String, Tuple(UInt64, UInt64))
) ENGINE = AggregatingMergeTree
ORDER BY (project_id, actor_name, actor_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS actor_inventory_updates TO actor_inventory AS
SELECT project_id, actor_name, actor_id,
       argMaxState(event_json, tuple(owner_epoch, sequence)) AS event
FROM actor_lifecycle
GROUP BY project_id, actor_name, actor_id;

ALTER TABLE actor_lifecycle MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

-- Pod usage from the node collector's kubeletstats receiver. Columns match the exporter (v0.163.0),
-- which inserts by name. SandboxId matches the control plane's pod identity (namespace/name/UID).
CREATE TABLE IF NOT EXISTS otel_metrics_gauge (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta, ZSTD(1)),
    TimeUnix DateTime CODEC(Delta, ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    Exemplars Nested (
        FilteredAttributes Map(LowCardinality(String), String),
        TimeUnix DateTime,
        Value Float64,
        SpanId String,
        TraceId String
    ) CODEC(ZSTD(1)),
    SandboxId String MATERIALIZED concat(ResourceAttributes['k8s.namespace.name'], '/',
        ResourceAttributes['k8s.pod.name'], '/', ResourceAttributes['k8s.pod.uid']),
    ReceivedAt DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (SandboxId, MetricName, TimeUnix)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

CREATE TABLE IF NOT EXISTS otel_metrics_sum (
    ResourceAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ResourceSchemaUrl String CODEC(ZSTD(1)),
    ScopeName String CODEC(ZSTD(1)),
    ScopeVersion String CODEC(ZSTD(1)),
    ScopeAttributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    ScopeDroppedAttrCount UInt32 CODEC(ZSTD(1)),
    ScopeSchemaUrl String CODEC(ZSTD(1)),
    ServiceName LowCardinality(String) CODEC(ZSTD(1)),
    MetricName LowCardinality(String) CODEC(ZSTD(1)),
    MetricDescription String CODEC(ZSTD(1)),
    MetricUnit String CODEC(ZSTD(1)),
    Attributes Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    StartTimeUnix DateTime CODEC(Delta, ZSTD(1)),
    TimeUnix DateTime CODEC(Delta, ZSTD(1)),
    Value Float64 CODEC(ZSTD(1)),
    Flags UInt32 CODEC(ZSTD(1)),
    Exemplars Nested (
        FilteredAttributes Map(LowCardinality(String), String),
        TimeUnix DateTime,
        Value Float64,
        SpanId String,
        TraceId String
    ) CODEC(ZSTD(1)),
    AggregationTemporality Int32 CODEC(ZSTD(1)),
    IsMonotonic Boolean CODEC(Delta, ZSTD(1)),
    SandboxId String MATERIALIZED concat(ResourceAttributes['k8s.namespace.name'], '/',
        ResourceAttributes['k8s.pod.name'], '/', ResourceAttributes['k8s.pod.uid']),
    ReceivedAt DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
PARTITION BY toDate(ReceivedAt)
ORDER BY (SandboxId, MetricName, TimeUnix)
TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND
SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1;

ALTER TABLE otel_metrics_gauge MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;
ALTER TABLE otel_metrics_sum MODIFY TTL toDateTime(ReceivedAt) + INTERVAL RETENTION_SECONDS SECOND;

-- Which actor ran in each sandbox, recorded by the control plane when it assigns one.
CREATE TABLE IF NOT EXISTS actor_sandboxes (
    project_id String,
    actor_name String,
    actor_id String,
    sandbox_id String,
    assigned_at_ms UInt64,
    memory_limit_bytes Nullable(UInt64),
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY (project_id, actor_name, actor_id, sandbox_id)
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

ALTER TABLE actor_sandboxes MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

CREATE TABLE IF NOT EXISTS sandbox_kills (
    sandbox_id String,
    killed_at_ms UInt64,
    reason LowCardinality(String),
    message String,
    received_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree
ORDER BY sandbox_id
TTL received_at + INTERVAL RETENTION_SECONDS SECOND;

ALTER TABLE sandbox_kills MODIFY TTL received_at + INTERVAL RETENTION_SECONDS SECOND;
