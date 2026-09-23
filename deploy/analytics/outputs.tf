output "runtime_environment" {
  value = {
    DURABLE_ACTORS_ANALYTICS_ENVIRONMENT    = var.environment
    DURABLE_ACTORS_ANALYTICS_PUBSUB_TOPIC   = google_pubsub_topic.events.id
    DURABLE_ACTORS_ANALYTICS_BQ_TABLE       = "${var.project_id}.${local.dataset}.${google_bigquery_table.events.table_id}"
    DURABLE_ACTORS_ANALYTICS_BQ_PROJECT     = var.project_id
    DURABLE_ACTORS_ANALYTICS_BQ_LOCATION    = var.location
    DURABLE_ACTORS_ANALYTICS_RETENTION_DAYS = tostring(var.retention_days)
  }
}
output "dead_letter_subscription" { value = google_pubsub_subscription.dead_letter.id }
output "bigquery_subscription" { value = google_pubsub_subscription.bigquery.id }
