import os
import dotenv
from airflow.models.dag import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from src.ingestion import extract_data_api, write_to_db
from src.init import create_db_conn
from airflow.decorators import dag, task

dotenv.load_dotenv()

# with DAG(
#     "etl_example",
#     description="Extract data from API, save to DB and aggregate it",
#     schedule_interval=None,
#     start_date=days_ago(2),
#     tags=["API-POSTGRES"],
#     catchup=False,
# ) as dag:

#     # Creates the connection between Airflow and postgres for each of the layers
#     db_conn_id = create_db_conn(
#         os.getenv("DB_HOST"),
#         os.getenv("DB_USER"),
#         os.getenv("DB_PORT"),
#         os.getenv("DB_PASS"),
#         os.getenv("DB_SCHEMA"),
#     )

#     # Extracts data from the API and saves it to bronze layer in the DB
#     extract_data_api_task = PythonOperator(
#         task_id="extract_data_api",
#         python_callable=extract_data_api,
#         op_kwargs={
#             "endpoint": os.getenv("API_ENDPOINT"),
#         },
#     )
#     write_to_db_task = PythonOperator(
#         task_id="write_to_db",
#         python_callable=write_to_db,
#         op_kwargs={
#             "df":None,
#             "db":os.getenv("DB_NAME"),
#             "host":"localhost",
#             "port":os.getenv("DB_PORT"),
#             "table":os.getenv("DB_TABLE"),
#             "schema":os.getenv("DB_SCHEMA"),
#             "user":os.getenv("DB_USER"),
#             "password":os.getenv("DB_PASS"),
#         },
#     )
#     # Cleans bronze layer data and saves it to silver layer
#     clean_data_db_task = PostgresOperator(
#         task_id="clean_data_db",
#         postgres_conn_id=db_conn_id,
#         sql="./db/clean_data.sql",
#     )

#     extract_data_api_task >> write_to_db_task >> clean_data_db_task


@dag(schedule=None, default_args={}, catchup=False)
def etl_example():
    
    dotenv.load_dotenv('/app/.env')
    import logging
    logging.info([ os.getenv("DB_NAME"),
            "localhost",
            os.getenv("DB_PORT"),
            os.getenv("DB_TABLE"),
            os.getenv("DB_SCHEMA"),
            os.getenv("DB_USER"),
            os.getenv("DB_PASS"),])
    logging.info( os.path.join(os.path.dirname(__file__))  )


    @task
    def extract_data_api_task():
        """
        Extracts data from the API and saves it to bronze layer in the DB
        """
        return extract_data_api(os.getenv("API_ENDPOINT"))

    @task
    def write_to_db_task(df):
        """
        Saves dataframe into DB
        """

       
        write_to_db(
            df,
            os.getenv("DB_NAME"),
            "localhost",
            os.getenv("DB_PORT"),
            os.getenv("DB_TABLE"),
            os.getenv("DB_SCHEMA"),
            os.getenv("DB_USER"),
            os.getenv("DB_PASS"),
        )

    # Invoke functions to create tasks and define dependencies
    write_to_db_task(extract_data_api_task())

etl_example()
