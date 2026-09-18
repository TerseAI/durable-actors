ALTER TABLE durable_object_deployment
    ADD COLUMN secret_refs text[] NOT NULL DEFAULT '{}';
