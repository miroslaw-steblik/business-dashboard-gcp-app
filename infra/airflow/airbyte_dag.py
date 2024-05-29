from __future__ import annotations



from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.utils.dates import days_ago



AIRBYTE_CONNECTION_ID = 'business-dashboard-airbyte'
CONN_ID = 'dde4de92-cf7e-461a-b237-cf069ce07d4a'
DAG_ID = 'business_dashboard_airbyte_dag'

with DAG(dag_id=DAG_ID,
        default_args={'owner': 'admin'},
        schedule_interval='@weekly',
        start_date=days_ago(1)
   ) as dag:
    
    sync_source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_source_destination',
        connection_id=CONN_ID,
        airbyte_conn_id = AIRBYTE_CONNECTION_ID
    )

# This DAG will trigger a sync job for the Airbyte connection with the ID 'business-dashboard-airbyte' 
# and wait for the job to complete.

# Path: ~/airflow/dags/airbyte_dag.py
# localhost:8080

