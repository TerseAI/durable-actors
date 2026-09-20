ALTER TABLE durable_object_spares ADD COLUMN kind TEXT NOT NULL DEFAULT 'actor'
    CHECK (kind IN ('actor', 'replica'));
