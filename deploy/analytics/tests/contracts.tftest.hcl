mock_provider "google" {}
mock_provider "google-beta" {
  mock_resource "google_project_service_identity" {
    defaults = { email = "service-123@gcp-sa-pubsub.iam.gserviceaccount.com" }
  }
}
variables {
  project_id              = "analytics-test"
  environment             = "test"
  location                = "US"
  pubsub_regions          = ["us-central1"]
  runtime_service_account = "runtime@analytics-test.iam.gserviceaccount.com"
}
run "ingestion_contract" {
  command = plan
  assert {
    condition     = google_pubsub_subscription.bigquery.bigquery_config[0].use_table_schema && google_pubsub_subscription.bigquery.bigquery_config[0].write_metadata && !google_pubsub_subscription.bigquery.bigquery_config[0].drop_unknown_fields
    error_message = "Export must preserve all schema fields and subscription metadata."
  }
  assert {
    condition     = google_bigquery_table.events.require_partition_filter && google_bigquery_table.events.time_partitioning[0].expiration_ms == 2592000000
    error_message = "Queries must be partition bounded and events retained for 30 days by default."
  }
  assert {
    condition     = google_pubsub_subscription.dead_letter.message_retention_duration == "604800s" && google_pubsub_subscription.bigquery.dead_letter_policy[0].max_delivery_attempts == 10
    error_message = "Failed exports must have a retained recovery subscription."
  }
  assert {
    condition     = google_project_iam_custom_role.writer.permissions == toset(["bigquery.tables.get", "bigquery.tables.updateData"])
    error_message = "The ingestion identity must have only destination table access."
  }
}
