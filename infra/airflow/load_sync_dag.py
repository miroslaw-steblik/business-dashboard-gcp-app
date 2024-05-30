from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
import pendulum

from airflow.operators.python import PythonOperator
from airflow.models import Variable


import os
import psycopg2

AIRBYTE_CONNECTION_ID = 'business-dashboard-airbyte'
# CONN_ID = 'dde4de92-cf7e-461a-b237-cf069ce07d4a'
DAG_ID = 'business_dashboard_airbyte_dag'


CLIENT_MAIN_PATH = Variable.get("CLIENT_MAIN_PATH")
AUM_PATH = Variable.get("AUM_PATH")
FEE_PATH = Variable.get("FEE_PATH")

CONN_ID = Variable.get("CONN_ID")



def load_postgres_db():
    print('Loading Postgres DB')

    # Connect to the PostgreSQL database server
    conn = psycopg2.connect(
        host=Variable.get("DB__HOST"),
        database=Variable.get("DB__DBNAME"),
        user=Variable.get("DB__USER"),
        password=Variable.get("DB__PASSWORD"),
        port=Variable.get("DB__PORT")
    )

    # Create a new cursor
    cur = conn.cursor()

    # A simple query to create a table
    cur.execute("DROP TABLE IF EXISTS client_main CASCADE;")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS client_main (
            internal_id VARCHAR PRIMARY KEY,
            company_name VARCHAR(255),
            product_type VARCHAR(255),
            status VARCHAR(255),
            client_representative VARCHAR(255),
            contact_name VARCHAR(255),
            business_type VARCHAR(255),
            website VARCHAR(255),
            description TEXT, 
            city VARCHAR(255),
            email VARCHAR(255),
            onboarding_date DATE,
            created_at DATE
        )
        """
    )

    conn.commit()

    cur.execute("TRUNCATE client_main;") # Truncate the table before loading the data

    with open(CLIENT_MAIN_PATH, "r") as f:
        next(f)             # Skip the header row.
        cur.copy_expert("COPY client_main FROM STDIN CSV", f)
    conn.commit()
    print("Data loaded into client_main table")

    # aum table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS client_aum (
            UUID SERIAL PRIMARY KEY,
            internal_id VARCHAR REFERENCES client_main( internal_id),
            aum_date DATE,
            aum NUMERIC,
            contributions NUMERIC,
            active_members NUMERIC,
            deferred_members NUMERIC,
            created_at DATE
        )
        """
    ) 

    conn.commit()

    cur.execute("TRUNCATE client_aum;") # Truncate the table before loading the data

    for file in os.listdir(AUM_PATH):
        with open(os.path.join(AUM_PATH, file), "r") as f:
            next(f)             # Skip the header row.
            cur.copy_expert("COPY client_aum( \
                            internal_id, \
                            aum_date, \
                            aum, \
                            contributions, \
                            active_members, \
                            deferred_members, \
                            created_at) \
                            FROM STDIN CSV", f)
        conn.commit()
        print(f"{file} loaded")

    # fee table
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS client_fee (
            UUID SERIAL PRIMARY KEY,
            internal_id VARCHAR REFERENCES client_main( internal_id),
            fee_date DATE,
            aum_bps NUMERIC,
            additional_fee_bps NUMERIC,
            created_at DATE
        )
        """
    )
    conn.commit()

    cur.execute("TRUNCATE client_fee;") # Truncate the table before loading the data

    for file in os.listdir(FEE_PATH):
        with open(os.path.join(FEE_PATH, file), "r") as f:
            next(f)             # Skip the header row.
            cur.copy_expert("COPY client_fee( \
                            internal_id, \
                            fee_date, \
                            aum_bps, \
                            additional_fee_bps, \
                            created_at) \
                            FROM STDIN CSV", f)
        conn.commit()
        print(f"{file} loaded")

    # Close the cursor and connection
    cur.close()
    conn.close()
    print('Postgres DB loaded')
    print("Connection closed")



with DAG(dag_id=DAG_ID,
        default_args={'owner': 'admin'},
        schedule='@weekly',
        start_date=pendulum.today().add(days=-1)
        
   ) as dag:
    
    
    load_postgres_task = PythonOperator(
        task_id='load_postgres_db',
        python_callable=load_postgres_db,
        dag=dag
    )
        

    sync_source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_source_destination',
        connection_id=CONN_ID,
        airbyte_conn_id = AIRBYTE_CONNECTION_ID
    )


   
load_postgres_task >> sync_source_destination


# This DAG will trigger a sync job for the Airbyte connection with the ID 'business-dashboard-airbyte' 
# and wait for the job to complete.

# Path: ~/airflow/dags/airbyte_dag.py
# localhost:8080

