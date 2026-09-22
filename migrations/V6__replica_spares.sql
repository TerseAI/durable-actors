ALTER TABLE durable_actors_spares ADD COLUMN kind TEXT NOT NULL DEFAULT 'actor'
    CHECK (kind IN ('actor', 'replica'));
