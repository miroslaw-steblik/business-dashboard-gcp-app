""" This script loads the data from the csv files to the Google Cloud BigQuery database."""
# not working yet - need to fix the schema

import os
from google.cloud import bigquery
from google.oauth2 import service_account

#------------------------- BigQuery Connection --------------------------------------#

# Set Google Cloud credentials and project
credentials = service_account.Credentials.from_service_account_file(
    '.credentials/streamlit-apps-424120-3699e75c183b.json')  
project_id = 'streamlit-apps-424120'  

# Initialize a BigQuery client
client = bigquery.Client(credentials=credentials, project=project_id)

# Set the dataset_id to the ID of the dataset to create.
dataset_id = 'business_db'  

#-------------------- client_main table --------------------------------------#

table_id = 'client_main'

# Set the full table id (project.dataset.table)
client_main_table_id = f"{project_id}.{dataset_id}.{table_id}"

# Set the job configuration
job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("internal_id", "STRING"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("product_type", "STRING"),
        bigquery.SchemaField("status", "STRING"),
        bigquery.SchemaField("client_representative", "STRING"),
        bigquery.SchemaField("contact_name", "STRING"),
        bigquery.SchemaField("business_type", "STRING"),
        bigquery.SchemaField("website", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("email", "STRING"),
        bigquery.SchemaField("onboarding_date", "DATE"),
        bigquery.SchemaField("created_at", "DATE"),
    ],
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)

with open("./data/client_database.csv", "rb") as source_file:
    job = client.load_table_from_file(source_file, client_main_table_id, job_config=job_config)

job.result()  

table = client.get_table(client_main_table_id) 
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {client_main_table_id}")

### aum table
table_id = 'client_aum'

# Set the full table id (project.dataset.table)
aum_table_id = f"{project_id}.{dataset_id}.{table_id}"

# Set the job configuration

job_config = bigquery.LoadJobConfig(
    schema=[
        # Specify the structure of your table here
        bigquery.SchemaField("internal_id", "STRING"),
        bigquery.SchemaField("aum_date", "DATE"),
        bigquery.SchemaField("aum", "NUMERIC"),
        bigquery.SchemaField("contributions", "NUMERIC"),
        bigquery.SchemaField("active_members", "NUMERIC"),
        bigquery.SchemaField("deferred_members", "NUMERIC"),
        bigquery.SchemaField("created_at", "DATE"),
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

for file in os.listdir("./data/aum"):
    with open(f"./data/aum/{file}", "rb") as source_file:
        job = client.load_table_from_file(source_file, aum_table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(aum_table_id)  # Make an API request.

print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {aum_table_id}")


### fee table
table_id = 'client_fee'

# Set the full table id (project.dataset.table)
fee_table_id = f"{project_id}.{dataset_id}.{table_id}"

# Set the job configuration

job_config = bigquery.LoadJobConfig(
    schema=[
        # Specify the structure of your table here
        bigquery.SchemaField("internal_id", "STRING"),
        bigquery.SchemaField("fee_date", "DATE"),
        bigquery.SchemaField("aum_bps", "NUMERIC"),
        bigquery.SchemaField("additional_fee_bps", "NUMERIC"),
        bigquery.SchemaField("created_at", "DATE"),
    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
)

for file in os.listdir("./data/fees"):
    with open(f"./data/fees/{file}", "rb") as source_file:
        job = client.load_table_from_file(source_file, fee_table_id, job_config=job_config)

job.result()  # Waits for the job to complete.

table = client.get_table(fee_table_id)  # Make an API request.

print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {fee_table_id}")

