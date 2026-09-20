CREATE TABLE durable_object_deployment (
    project_id TEXT PRIMARY KEY,
    code_revision TEXT NOT NULL,
    image_ref TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    actor_entrypoint TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
