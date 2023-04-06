locals {
  data_lake_bucket = "bechdel-project_raw-storage"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "zoomcamp-user"
  type = string
}

variable "region" {
  description = "Region for GCP resources."
  default = "us-west1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "bechdel-project"
}