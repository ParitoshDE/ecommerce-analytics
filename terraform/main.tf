terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "local" {}
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ---------------------------------------------------------------
# Service Account
# ---------------------------------------------------------------
resource "google_service_account" "pipeline_sa" {
  account_id   = "olist-pipeline-sa"
  display_name = "Olist E-Commerce Analytics pipeline service account"
}

resource "google_project_iam_member" "sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ---------------------------------------------------------------
# GCS Bucket — Data Lake
# ---------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name          = var.data_lake_bucket_name
  location      = var.region
  force_destroy = true
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 90
    }
  }
}

# ---------------------------------------------------------------
# BigQuery Datasets
# ---------------------------------------------------------------
resource "google_bigquery_dataset" "raw" {
  dataset_id  = var.raw_dataset_id
  location    = var.region
  description = "Olist raw layer — denormalized orders_enriched table loaded from GCS"
}

resource "google_bigquery_dataset" "prod" {
  dataset_id  = var.prod_dataset_id
  location    = var.region
  description = "Olist production layer — dbt models (staging, dimensions, facts, aggregations)"
}
