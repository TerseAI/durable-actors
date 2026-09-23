variable "project_id" {
  type = string
}
variable "environment" {
  type = string
  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,30}$", var.environment))
    error_message = "Use a lowercase environment identifier, at most 31 characters."
  }
}
variable "location" {
  type        = string
  description = "BigQuery dataset location. Choose the same geography as the runtime."
}
variable "pubsub_regions" {
  type        = list(string)
  description = "GCP regions allowed to store telemetry messages."
  validation {
    condition     = length(var.pubsub_regions) > 0
    error_message = "At least one Pub/Sub storage region is required."
  }
}
variable "runtime_service_account" {
  type        = string
  description = "Existing control-plane service account email; no keys are created."
}
variable "retention_days" {
  type    = number
  default = 30
  validation {
    condition     = var.retention_days >= 1 && var.retention_days <= 365 && floor(var.retention_days) == var.retention_days
    error_message = "Retention must be an integer from 1 to 365 days."
  }
}
variable "notification_channels" {
  type        = list(string)
  default     = []
  description = "Existing Cloud Monitoring channels to receive backlog and dead-letter alerts."
}
