variable "gcp_project_id" {
  type = string
  description = "The GCP Project ID where Airbyte will load data"
}

variable "airbyte_workspace_id" {
  type = string
  description = "The UUID of your airbyte workspace."
}



variable "gcp_hmac_access_key" {
    type = string
}

variable "gcp_hmac_secret" {
    type = string
}

variable "dataset_id" {
    type = string
    default = "raw_data"
}

variable "dataset_location" {
    type = string
}

variable "credentials_json_path" {
    type = string
    description = "Google Cloud service account credentials"
}

variable "gcs_bucket_name" {
    type = string
}

variable "gcs_bucket_path" {
    type = string
}

variable "DB__HOST" {
    type = string
}

variable "DB__DBNAME" {
    type = string
}

variable "DB__USER" {
    type = string
}

variable "DB__PASSWORD" {
    type = string
}