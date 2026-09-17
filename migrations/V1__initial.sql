CREATE TABLE IF NOT EXISTS durable_object_namespaces (
    namespace_id TEXT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS durable_object_project_specs (
    namespace_id TEXT PRIMARY KEY REFERENCES durable_object_namespaces(namespace_id),
    code_revision TEXT NOT NULL,
    image_ref TEXT NOT NULL,
    working_directory TEXT NOT NULL,
    actor_entrypoint TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
