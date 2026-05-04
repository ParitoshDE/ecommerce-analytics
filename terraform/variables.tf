variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "us-central1"
}

variable "data_lake_bucket_name" {
  description = "GCS bucket name for the data lake (must be globally unique)"
  type        = string
}

variable "raw_dataset_id" {
  description = "BigQuery dataset for raw Spark output"
  type        = string
  default     = "olist_raw"
}

variable "prod_dataset_id" {
  description = "BigQuery dataset for dbt production models"
  type        = string
  default     = "olist_prod"
}
