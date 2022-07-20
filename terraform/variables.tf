locals {
  data_lake_bucket = "levant-data-lake"
}

variable "project" {
  description = "Your GCP Project ID"
  default = "nyc-taxi-dwh"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-east4"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}


