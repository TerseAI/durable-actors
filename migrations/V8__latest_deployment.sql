ALTER TABLE durable_actors_deployment DROP COLUMN code_revision;
ALTER TABLE durable_actors_contracts DROP COLUMN code_revision;
ALTER TABLE durable_actors_spares RENAME COLUMN code_revision TO host_config_key;
