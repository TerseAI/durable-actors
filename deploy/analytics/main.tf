locals {
  name           = "actor-analytics-${var.environment}"
  dataset        = "actor_analytics_${var.environment}"
  runtime_member = "serviceAccount:${var.runtime_service_account}"
  pubsub_member  = "serviceAccount:${google_project_service_identity.pubsub.email}"
}
resource "google_project_service" "apis" {
  for_each           = toset(["pubsub.googleapis.com", "bigquery.googleapis.com", "monitoring.googleapis.com"])
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
resource "google_project_service_identity" "pubsub" {
  provider   = google-beta
  project    = var.project_id
  service    = "pubsub.googleapis.com"
  depends_on = [google_project_service.apis]
}
resource "google_bigquery_dataset" "analytics" {
  project                    = var.project_id
  dataset_id                 = local.dataset
  location                   = var.location
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.apis]
}
resource "google_bigquery_table" "events" {
  project                  = var.project_id
  dataset_id               = google_bigquery_dataset.analytics.dataset_id
  table_id                 = "trace_events_v1"
  deletion_protection      = true
  schema                   = file("${path.module}/schema.json")
  require_partition_filter = true
  clustering               = ["project_id", "actor_name", "actor_id", "connection_id"]
  time_partitioning {
    type          = "DAY"
    field         = "started_at"
    expiration_ms = var.retention_days * 86400000
  }
}
resource "google_pubsub_topic" "events" {
  project                    = var.project_id
  name                       = "${local.name}-v1"
  message_retention_duration = "604800s"
  message_storage_policy { allowed_persistence_regions = var.pubsub_regions }
  depends_on = [google_project_service.apis]
}
resource "google_pubsub_topic" "dead_letter" {
  project                    = var.project_id
  name                       = "${local.name}-dead-letter"
  message_retention_duration = "604800s"
  message_storage_policy { allowed_persistence_regions = var.pubsub_regions }
  depends_on = [google_project_service.apis]
}
resource "google_project_iam_custom_role" "writer" {
  project     = var.project_id
  role_id     = "actorAnalyticsWriter_${var.environment}"
  title       = "Actor analytics BigQuery ingestion"
  permissions = ["bigquery.tables.get", "bigquery.tables.updateData"]
}
resource "google_bigquery_table_iam_member" "writer" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = google_bigquery_table.events.table_id
  role       = google_project_iam_custom_role.writer.name
  member     = local.pubsub_member
}
resource "google_pubsub_topic_iam_member" "publisher" {
  project = var.project_id
  topic   = google_pubsub_topic.events.name
  role    = "roles/pubsub.publisher"
  member  = local.runtime_member
}
resource "google_pubsub_topic_iam_member" "dead_letter_writer" {
  project = var.project_id
  topic   = google_pubsub_topic.dead_letter.name
  role    = "roles/pubsub.publisher"
  member  = local.pubsub_member
}
resource "google_pubsub_subscription" "bigquery" {
  project                    = var.project_id
  name                       = "${local.name}-bigquery-v1"
  topic                      = google_pubsub_topic.events.id
  message_retention_duration = "604800s"
  expiration_policy { ttl = "" }
  bigquery_config {
    table               = "${var.project_id}.${local.dataset}.${google_bigquery_table.events.table_id}"
    use_table_schema    = true
    write_metadata      = true
    drop_unknown_fields = false
  }
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 10
  }
  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
  depends_on = [google_bigquery_table_iam_member.writer, google_pubsub_topic_iam_member.dead_letter_writer]
}
resource "google_pubsub_subscription_iam_member" "forwarder" {
  project      = var.project_id
  subscription = google_pubsub_subscription.bigquery.name
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_member
}
resource "google_pubsub_subscription" "dead_letter" {
  project                    = var.project_id
  name                       = "${local.name}-dead-letter-retained"
  topic                      = google_pubsub_topic.dead_letter.id
  message_retention_duration = "604800s"
  expiration_policy { ttl = "" }
}
resource "google_bigquery_table_iam_member" "reader" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  table_id   = google_bigquery_table.events.table_id
  role       = "roles/bigquery.dataViewer"
  member     = local.runtime_member
}
resource "google_project_iam_member" "query_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = local.runtime_member
}
