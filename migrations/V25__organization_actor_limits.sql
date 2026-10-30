CREATE TABLE durable_actors_organization_limits (
    organization_id TEXT PRIMARY KEY,
    max_active_actors BIGINT CHECK (max_active_actors BETWEEN 1 AND 4294967295),
    actor_starts_per_second BIGINT CHECK (actor_starts_per_second BETWEEN 1 AND 4294967295),
    actor_start_burst BIGINT CHECK (actor_start_burst BETWEEN 1 AND 4294967295)
);

-- An organization inherits the most generous override among its projects.
INSERT INTO durable_actors_organization_limits
    (organization_id, max_active_actors, actor_starts_per_second, actor_start_burst)
SELECT d.organization_id, MAX(l.max_active_actors), MAX(l.actor_starts_per_second), MAX(l.actor_start_burst)
FROM durable_actors_project_limits l
JOIN durable_actors_deployment d ON d.project_id = l.project_id
WHERE d.organization_id IS NOT NULL
GROUP BY d.organization_id;

DROP TABLE durable_actors_project_limits;
