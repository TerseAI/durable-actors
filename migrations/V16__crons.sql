CREATE TABLE durable_actors_cron_definitions (
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    method TEXT NOT NULL,
    expression TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    retries INTEGER NOT NULL DEFAULT 0 CHECK (retries >= 0),
    PRIMARY KEY (project_id, actor_name, method, expression)
);

CREATE TABLE durable_actors_cron_instances (
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    region TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (project_id, actor_name, actor_id)
);

CREATE TABLE durable_actors_crons (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    method TEXT NOT NULL,
    expression TEXT NOT NULL,
    region TEXT NOT NULL,
    next_at BIGINT NOT NULL,
    available_at BIGINT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    token TEXT,
    lease_until BIGINT NOT NULL DEFAULT 0,
    UNIQUE (project_id, actor_name, actor_id, method, expression),
    FOREIGN KEY (project_id, actor_name, method, expression)
        REFERENCES durable_actors_cron_definitions ON DELETE CASCADE,
    FOREIGN KEY (project_id, actor_name, actor_id)
        REFERENCES durable_actors_cron_instances ON DELETE CASCADE
);

CREATE INDEX durable_actors_crons_due ON durable_actors_crons (available_at, lease_until);

CREATE TABLE durable_actors_cron_reconciliation (
    id INTEGER PRIMARY KEY,
    available_at BIGINT NOT NULL
);

INSERT INTO durable_actors_cron_reconciliation (id, available_at) VALUES (1, 0);
