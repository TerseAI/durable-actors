# GCP analytics deployment

This module provisions the managed Pub/Sub → BigQuery pipeline. It does not deploy or modify Terse. Use a dedicated module instance per environment and data geography.

```hcl
module "analytics" {
  source                  = "./deploy/analytics"
  project_id              = "your-gcp-project"
  environment             = "production"
  location                = "US"
  pubsub_regions          = ["us-central1"]
  runtime_service_account = "actor-control-plane@your-gcp-project.iam.gserviceaccount.com"
  notification_channels   = ["projects/your-gcp-project/notificationChannels/CHANNEL"]
}
```

Apply through your usual Terraform deployment process. The caller needs permission to enable APIs, create the resources, and manage the specified IAM bindings. No service-account keys are created. The BigQuery table has deletion protection; retain that setting for production.

Copy the `runtime_environment` output into the control plane's deployment environment and configure ADC for its service identity. Use the same runtime secret across replicas. The identity receives publication on the topic, read access to this table, and query-job creation in the GCP project. The Pub/Sub service agent receives a custom role with only `bigquery.tables.get` and `bigquery.tables.updateData` on the destination table, plus the forwarding permissions for dead letters.

| Environment variable                            | Value/default                                                          |
| ----------------------------------------------- | ---------------------------------------------------------------------- |
| `DURABLE_ACTORS_ANALYTICS_PUBSUB_TOPIC`         | Required full topic resource name                                      |
| `DURABLE_ACTORS_ANALYTICS_BQ_TABLE`             | Required `project.dataset.table`                                       |
| `DURABLE_ACTORS_ANALYTICS_BQ_PROJECT`           | Required query billing project                                         |
| `DURABLE_ACTORS_ANALYTICS_BQ_LOCATION`          | Required dataset/job location                                          |
| `DURABLE_ACTORS_ANALYTICS_ENVIRONMENT`          | Required environment label                                             |
| `DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS`       | 30; must match Terraform, range 1–365                                  |
| `DURABLE_ACTORS_ANALYTICS_MAXIMUM_BYTES_BILLED` | 1,073,741,824 bytes per query; tune after measuring                    |
| `DURABLE_ACTORS_ANALYTICS_CACHE_SECONDS`        | 15; range 5–300                                                        |
| `DURABLE_ACTORS_ANALYTICS_METADATA_FIELDS`      | Omitted by default; comma-separated top-level connection metadata keys |
| `DURABLE_ACTORS_SECRET`                         | Required backend-only admin secret and cursor signing key              |

Omit all analytics settings to use SQLite. Partial analytics settings fail startup. Upgrade hosts with the control plane so producer event IDs survive retransmission. Topic/schema access is checked by actual publication/query calls, not by adding broad management permissions to the runtime.

Backend calls use `/v1/projects/PROJECT/observe/...` and `Authorization: Bearer RUNTIME_SECRET`. Terse must authorize the user for that project before proxying. The observer UI's existing injected HTTP client and DTOs can be reused.

## Verification

```sh
terraform -chdir=deploy/analytics init -backend=false
terraform -chdir=deploy/analytics validate
terraform -chdir=deploy/analytics test
cargo test --locked --lib request_traces
```

For an isolated GCP environment, provision the module, export its analytics environment settings, and set up ADC. The following test publishes a small number of retained test events under unique project IDs; it does not create resources or delete data:

```sh
cargo test --locked --lib pubsub_bigquery_end_to_end_matches_sqlite -- --ignored --nocapture
```

It waits up to two minutes for visibility and compares duplicate handling, project isolation, exact metrics, queue waits, sessions, metadata, and history pagination against SQLite. Run it before enabling hosted reads. Test retention/schema-failure/dead-letter recovery separately during rollout; those operational exercises are not simulated by Terraform mocks.

## Operations and recovery

Configure notification channels: the module alerts on sustained ingestion backlog age above two minutes and on any retained dead-letter backlog. Also monitor control-plane publication warnings, full publisher queues, host-reported undelivered telemetry, query failures, native cache hit logs, processed bytes, and GCP billing. Missing recent customer events alone is not a reliable health check.

For dead letters, fix the schema or IAM cause first. Inspect messages from the retained dead-letter subscription with an operator identity. GCP wraps the original message; extract its original event data and republish it to the original topic with the same `event_id`, project, and logical trace fields. Acknowledge the dead letter only after publish acknowledgement. Queries deduplicate repeated delivery. Never substitute the new Pub/Sub message ID for the producer event ID.

Topic retention also permits seeking the export subscription back within seven days after fixing an ingestion issue. This intentionally replays duplicates. Events beyond BigQuery's retained UTC partitions cannot restore an expired history window; restore to a separate recovery table if needed. Add nullable schema columns before changing producers, and use a new versioned topic/table for breaking changes.

Keep the export subscription and retained dead-letter subscription present during incidents. Removing analytics settings stops collection of new events; it does not delete warehouse data. Preserve the warehouse and Pub/Sub backlog when rolling back application binaries. Coordinate a schema migration downgrade: the new local SQLite database version cannot be opened by an older binary; back up local observer history before reverting.
