CREATE TABLE durable_actors_trace_projects (
    project_id TEXT COLLATE "C" PRIMARY KEY,
    head BIGINT NOT NULL DEFAULT 0,
    evicted BIGINT NOT NULL DEFAULT 0,
    pruned BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE durable_actors_traces (
    project_id TEXT COLLATE "C" NOT NULL REFERENCES durable_actors_trace_projects(project_id),
    position BIGINT NOT NULL,
    event_id TEXT COLLATE "C" NOT NULL UNIQUE,
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
CREATE INDEX durable_actors_traces_outcome ON durable_actors_traces (project_id, outcome, started_at_ms DESC, position DESC);
CREATE INDEX durable_actors_traces_retention ON durable_actors_traces (received_at);
CREATE INDEX durable_actors_traces_sockets ON durable_actors_traces (project_id, connection_id, actor_name, actor_id, (operation = 'onConnect') DESC, started_at_ms, position) WHERE kind = 'websocket';
