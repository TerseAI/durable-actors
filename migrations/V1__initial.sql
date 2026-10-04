CREATE TABLE durable_actors_deployment (
    project_id TEXT PRIMARY KEY,
    image_ref TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    actor_entrypoint TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    secret_refs TEXT[] NOT NULL DEFAULT '{}',
    code_snapshot TEXT,
    source_json TEXT,
    sandbox_json TEXT NOT NULL DEFAULT '{}'
);

CREATE TABLE durable_actors_contracts (
    project_id TEXT PRIMARY KEY REFERENCES durable_actors_deployment(project_id) ON DELETE CASCADE,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL
);

CREATE TABLE durable_actors_trace_projects (
    project_id TEXT COLLATE "C" PRIMARY KEY,
    head BIGINT NOT NULL DEFAULT 0,
    evicted BIGINT NOT NULL DEFAULT 0,
    pruned BIGINT NOT NULL DEFAULT 0,
    dropped BIGINT NOT NULL DEFAULT 0,
    persistence_failed BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE durable_actors_traces (
    project_id TEXT COLLATE "C" NOT NULL REFERENCES durable_actors_trace_projects(project_id),
    position BIGINT NOT NULL,
    event_id TEXT COLLATE "C" NOT NULL UNIQUE,
    -- Request IDs can contain NUL bytes, which PostgreSQL text cannot store.
    request_id BYTEA NOT NULL,
    event TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at_ms BIGINT NOT NULL,
    actor_name TEXT COLLATE "C" NOT NULL,
    actor_id TEXT COLLATE "C" NOT NULL,
    outcome TEXT COLLATE "C" NOT NULL,
    duration_ms DOUBLE PRECISION NOT NULL,
    queue_wait_ms DOUBLE PRECISION,
    kind TEXT COLLATE "C" NOT NULL,
    operation TEXT COLLATE "C" NOT NULL,
    connection_id TEXT COLLATE "C",
    host_id TEXT COLLATE "C" NOT NULL,
    PRIMARY KEY (project_id, position)
);
CREATE INDEX durable_actors_traces_time ON durable_actors_traces (project_id, started_at_ms DESC, position DESC);
CREATE INDEX durable_actors_traces_actor ON durable_actors_traces (project_id, actor_name, actor_id, started_at_ms DESC, position DESC);
CREATE INDEX durable_actors_traces_request ON durable_actors_traces (project_id, request_id, started_at_ms DESC, position DESC);
CREATE INDEX durable_actors_traces_outcome ON durable_actors_traces (project_id, outcome, started_at_ms DESC, position DESC);
CREATE INDEX durable_actors_traces_retention ON durable_actors_traces (received_at);
CREATE INDEX durable_actors_traces_sockets ON durable_actors_traces (project_id, connection_id, actor_name, actor_id, (operation = 'onConnect') DESC, started_at_ms, position) WHERE kind = 'websocket';

CREATE TABLE durable_actors_storage_tokens (
    scope_key TEXT PRIMARY KEY,
    issuer TEXT NOT NULL,
    boundary JSONB NOT NULL,
    token TEXT,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    refresh_owner TEXT,
    refresh_until TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX durable_actors_storage_tokens_expiry ON durable_actors_storage_tokens (expires_at);

CREATE TABLE socket_gateways (
    id TEXT PRIMARY KEY,
    route TEXT NOT NULL,
    accepts_rooms BOOLEAN NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE socket_rooms (
    actor_key TEXT PRIMARY KEY,
    gateway_id TEXT NOT NULL REFERENCES socket_gateways(id) ON DELETE CASCADE
);
CREATE INDEX socket_rooms_gateway ON socket_rooms(gateway_id);
