CREATE TABLE durable_object_contracts (
    namespace_id TEXT NOT NULL REFERENCES durable_object_namespaces(namespace_id),
    code_revision TEXT NOT NULL,
    contract_hash TEXT NOT NULL,
    contract_json TEXT NOT NULL,
    PRIMARY KEY (namespace_id, code_revision)
);
