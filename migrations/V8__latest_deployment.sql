ALTER TABLE durable_object_deployment DROP COLUMN code_revision;
ALTER TABLE durable_object_contracts DROP COLUMN code_revision;
ALTER TABLE durable_object_spares RENAME COLUMN code_revision TO host_config_key;
