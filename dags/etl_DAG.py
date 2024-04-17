import os
import dotenv
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.ingestion import extract_data_api, write_to_db
from src.init import create_db_conn

dotenv.load_dotenv()

with DAG(
    "etl_example",
    description="Extract data from API and save to DB",
    schedule_interval=None,
    start_date=days_ago(2),
    tags=["API-POSTGRES"],
    catchup=False,
) as dag:

    # Creates the connection between Airflow and postgres for each of the layers
    silver_layer_conn_id = create_db_conn(
        os.getenv("DB_HOST"),
        os.getenv("DB_USER"),
        os.getenv("DB_PORT"),
        os.getenv("DB_PASS"),
        "silver_layer"
    )
    golden_layer_conn_id = create_db_conn(
        os.getenv("DB_HOST"),
        os.getenv("DB_USER"),
        os.getenv("DB_PORT"),
        os.getenv("DB_PASS"),
        "golden_layer"
    )
    
    # Extracts data from the API and saves it to bronze layer in the DB
    write_to_db_task = PythonOperator(
        task_id="extract_data_api",
        python_callable=write_to_db,
        op_kwargs={
            "endpoint": os.getenv("API_ENDPOINT"),
        },
    )

    extract_data_api_task = PythonOperator(
        task_id="extract_data_api",
        python_callable=extract_data_api,
        op_kwargs={
            "endpoint": os.getenv("API_ENDPOINT"),
        },
    )

    # Cleans bronze layer data and saves it to silver layer
    clean_data_db_task = PostgresOperator(
        task_id="clean_data_db",
        postgres_conn_id=silver_layer_conn_id,
        sql="./db/clean_data.sql",
    )

    # Agregates silver layer data and saves it to golden layer
    consolidate_data_db_task = PostgresOperator(
        task_id="consolidate_data_db",
        postgres_conn_id=golden_layer_conn_id,
        sql="./db/consolidate_data.sql",
    )

    extract_data_api_task >> clean_data_db_task >> consolidate_data_db_task
