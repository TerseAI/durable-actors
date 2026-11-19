CREATE TABLE actor_ownership (
    key TEXT PRIMARY KEY,
    generation BIGINT NOT NULL CHECK (generation > 0),
    record BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
