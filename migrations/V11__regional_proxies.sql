CREATE TABLE durable_actors_proxies (
    object_id TEXT NOT NULL,
    region TEXT NOT NULL,
    protocol_version INTEGER NOT NULL CHECK (protocol_version = 1),
    session_id TEXT NOT NULL,
    handle TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (object_id, region, protocol_version)
);
