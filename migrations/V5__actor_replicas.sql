CREATE TABLE durable_object_replica_groups (
    id TEXT PRIMARY KEY,
    config TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    checked_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
