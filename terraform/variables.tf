locals {
  data_lake_bucket = "bechdel_data_lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "data-project-3495"
  type = string
}

variable "region" {
  description = "Region for GCP resources."
  default = "us-central1"
  type = string
}

variable "credentials" {
  description = "Location of service account credential file"
  default = "~/keys/project_service_key.json"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket."
  default = "STANDARD"
  type = string
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default = "bechdel_movies_project"
  type = string
}