terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
    kestra = {
      source  = "kestra-io/kestra"
    }
    http = {
      source  = "hashicorp/http"
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


provider "kestra" {
  url   = "http://kestra:8080"
  username = "admin@kestra.io" # Default is often admin
  password = "Admin1234!"
}

resource "kestra_kv" "gcp_project_id" {
  namespace = "final_project"
  key       = "GCP_PROJECT_ID"
  value     = var.project
}

resource "kestra_kv" "gcp_location" {
  namespace = "final_project"
  key       = "GCP_LOCATION"
  value     = var.location
}

resource "kestra_kv" "gcp_bucket_name" {
  namespace = "final_project"
  key       = "GCP_BUCKET_NAME"
  value     = var.gcs_bucket_name
}

resource "kestra_kv" "gcp_dataset" {
  namespace = "final_project"
  key       = "GCP_DATASET"
  value     = var.bq_dataset_name
}


# Use this ONLY if you have Kestra Enterprise
resource "kestra_kv" "gcp_creds" {
  namespace = "final_project"
  key       = "GCP_CREDS"
  value     = file("${path.module}/${var.credentials}")
  type      = "JSON"
}


resource "kestra_kv" "kaggle_username" {
  namespace = "final_project"
  key       = "KAGGLE_USERNAME"
  value     = var.kaggle_username
}

resource "kestra_kv" "kaggle_key" {
  namespace = "final_project"
  key       = "KAGGLE_KEY"
  value     = var.kaggle_key
}