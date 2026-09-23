resource "google_monitoring_alert_policy" "backlog" {
  project               = var.project_id
  display_name          = "${local.name}: ingestion delayed"
  combiner              = "OR"
  notification_channels = var.notification_channels
  conditions {
    display_name = "Oldest unacknowledged event exceeds two minutes"
    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND resource.label.subscription_id = \"${google_pubsub_subscription.bigquery.name}\" AND metric.type = \"pubsub.googleapis.com/subscription/oldest_unacked_message_age\""
      comparison      = "COMPARISON_GT"
      threshold_value = 120
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
  depends_on = [google_project_service.apis]
}
resource "google_monitoring_alert_policy" "dead_letter" {
  project               = var.project_id
  display_name          = "${local.name}: quarantined events"
  combiner              = "OR"
  notification_channels = var.notification_channels
  conditions {
    display_name = "Dead-letter subscription is nonempty"
    condition_threshold {
      filter          = "resource.type = \"pubsub_subscription\" AND resource.label.subscription_id = \"${google_pubsub_subscription.dead_letter.name}\" AND metric.type = \"pubsub.googleapis.com/subscription/num_undelivered_messages\""
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "60s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
  depends_on = [google_project_service.apis]
}
