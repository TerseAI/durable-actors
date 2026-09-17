CREATE TABLE durable_object_contracts (
    namespace_id TEXT PRIMARY KEY REFERENCES durable_object_project_specs(namespace_id) ON DELETE CASCADE,
    code_revision TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL
);
