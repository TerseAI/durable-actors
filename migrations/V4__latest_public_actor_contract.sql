DELETE FROM durable_object_contracts AS contracts
WHERE NOT EXISTS (
    SELECT 1 FROM durable_object_project_specs AS deployments
    WHERE deployments.namespace_id = contracts.namespace_id
      AND deployments.code_revision = contracts.code_revision
);

ALTER TABLE durable_object_contracts
    DROP CONSTRAINT durable_object_contracts_pkey,
    ADD PRIMARY KEY (namespace_id),
    ADD FOREIGN KEY (namespace_id) REFERENCES durable_object_project_specs(namespace_id) ON DELETE CASCADE;
