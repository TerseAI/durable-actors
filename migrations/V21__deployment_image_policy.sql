ALTER TABLE durable_actors_deployment
    ADD COLUMN image_policy TEXT NOT NULL DEFAULT 'legacy'
    CHECK (image_policy IN ('platform', 'custom', 'legacy'));
