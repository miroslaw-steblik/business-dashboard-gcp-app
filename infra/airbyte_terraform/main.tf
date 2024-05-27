// Airbyte Terraform provider documentation: https://registry.terraform.io/providers/airbytehq/airbyte/latest/docs

// Sources
resource "airbyte_source_postgres" "my_source_postgres" {
  configuration = {
    database        = var.DB__DBNAME
    host            = var.DB__HOST
    password        = var.DB__PASSWORD
    port            = 5432
    replication_method = {
      detect_changes_with_xmin_system_column = {}
    }
    schemas = ["public"]
    username = var.DB__USER
  }

  name          = "my_source_postgres"
  workspace_id  = var.airbyte_workspace_id
}

// Destinations
resource "airbyte_destination_bigquery" "my_destination_bigquery" {
  configuration = {
    big_query_client_buffer_size_mb = 15
    credentials_json                = file(var.credentials_json_path)
    dataset_id                      = var.dataset_id
    dataset_location                = var.dataset_location
    disable_type_dedupe             = false
    loading_method = {
      gcs_staging = {
        credential = {
          hmac_key = {
            hmac_key_access_id = var.gcp_hmac_access_key
            hmac_key_secret    = var.gcp_hmac_secret
          }
        }
        gcs_bucket_name          = var.gcs_bucket_name
        gcs_bucket_path          = var.gcs_bucket_path
        keep_files_in_gcs_bucket = "Delete all tmp files from GCS"
      }
    }
    project_id              = var.gcp_project_id
    #raw_data_dataset        = ""
    transformation_priority = "interactive"
  }
  
  name          = "my_destination_bigquery"
  workspace_id  = var.airbyte_workspace_id
}




// Connections
resource "airbyte_connection" "postgres_to_bigquery" {
    name = "Postgres to BigQuery"
    source_id = airbyte_source_postgres.my_source_postgres.source_id
    destination_id = airbyte_destination_bigquery.my_destination_bigquery.destination_id
    configurations = {
        streams = [
            {
                name = "client_aum"
            },
            {
                name = "client_fee"
            },
            {
                name = "client_main"
            },
        ]
    }
}