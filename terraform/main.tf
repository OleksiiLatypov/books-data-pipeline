terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.22.0"
    }
  }
}

provider "google" {
  credentials = file("${path.module}/${var.credentials}")
  project     = var.project
  region      = var.region # compute region
}

data "google_storage_bucket" "existing_bucket" {
  name = var.gcs_bucket_name
}

resource "google_storage_bucket" "books-bucket" {
  count         = length(data.google_storage_bucket.existing_bucket) == 0 ? 1 : 0
  name          = var.gcs_bucket_name
  location      = var.location # storage location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 3
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  storage_class = var.gcs_storage_class
}

data "google_bigquery_dataset" "existing_dataset" {
  dataset_id = var.bq_dataset_name
}

resource "google_bigquery_dataset" "demo-dataset" {
  count      = length(data.google_bigquery_dataset.existing_dataset) == 0 ? 1 : 0
  dataset_id = var.bq_dataset_name
  location   = var.location # dataset location

  lifecycle {
    prevent_destroy = false 
  }
}