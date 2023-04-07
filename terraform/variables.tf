locals {
  data_lake_bucket = "bechdel-project_data-lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "data-projects-383009"
  type = string
}

variable "region" {
  description = "Region for GCP resources."
  default = "us-west1"
  type = string
}

variable "credentials" {
  description = "Location of service account credential file"
  default = "/home/jdtganding/Documents/bechdel-movies-project/keys/project_service_key.json"
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