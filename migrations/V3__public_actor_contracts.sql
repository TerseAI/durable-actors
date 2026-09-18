CREATE TABLE durable_object_contracts (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton) REFERENCES durable_object_deployment(singleton) ON DELETE CASCADE,
    code_revision TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL
);
