from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum

AIRBYTE_CONNECTION_ID = 'business-dashboard-airbyte'


with DAG(dag_id='business_dashboard_airbyte_dag',
        default_args={'owner': 'airflow'},
        schedule='@daily',
        start_date=pendulum.today('UTC').add(days=-1)
   ) as dag:

   trigger_airbyte_sync = AirbyteTriggerSyncOperator(
       task_id='airbyte_trigger_sync',
       airbyte_conn_id='dde4de92-cf7e-461a-b237-cf069ce07d4a',
       connection_id=AIRBYTE_CONNECTION_ID,
       asynchronous=True
   )

   wait_for_sync_completion = AirbyteJobSensor(
       task_id='airbyte_check_sync',
       airbyte_conn_id='dde4de92-cf7e-461a-b237-cf069ce07d4a',
       airbyte_job_id=trigger_airbyte_sync.output
   )


   
   trigger_airbyte_sync >> wait_for_sync_completion 

# This DAG will trigger a sync job for the Airbyte connection with the ID 'business-dashboard-airbyte' 
# and wait for the job to complete.

# Path: ~/airflow/dags/airbyte_dag.py
# localhost:8080

