CREATE TABLE durable_object_contracts (
    project_id TEXT PRIMARY KEY REFERENCES durable_object_deployment(project_id) ON DELETE CASCADE,
    code_revision TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL
);
