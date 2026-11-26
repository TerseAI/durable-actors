-- One ownership record per actor; `version` is its compare-and-swap token.
CREATE TABLE actor_ownership (
    project_id TEXT NOT NULL,
    actor_name TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    version BIGINT NOT NULL,
    sealed BOOLEAN NOT NULL,
    record JSONB NOT NULL,
    PRIMARY KEY (project_id, actor_name, actor_id)
) WITH (fillfactor = 80);

-- Lease renewals change no indexed column, so they stay heap-only updates.
CREATE INDEX actor_ownership_unsealed ON actor_ownership (project_id) WHERE NOT sealed;
