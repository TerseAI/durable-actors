ALTER TABLE durable_actors_deployment
    ADD COLUMN secret_refs text[] NOT NULL DEFAULT '{}';
