# Template ETL with Airflow, Spark and Postgres
Simple tamplate for ETLs using Postgres, Spark and Airflow deployed on docker containers.

To deploy simply run:
```
docker-compose up
```
After that log into the Airflow interface at http://localhost:8080/home using the credentials:

    user: airflow
    password: airflow


The databases and tables in Postgres are created by the sript [/dags/db/init.sql](./dags/db/init.sql) when the container is created by Docker Compose, and the credentials are located in the enviroment file [/dags/.env ](./dags/.env) (in a real environment, this file should not be added to git).

The [Dockerfile](./Dockerfile) pulls and Airflow docker image and installs the required Python packages described at [requirements.txt](./requirements.txt).

The ETL DAG is located at  [/dags/etl_DAG.py](./dags/etl_DAG.py) and as a demonstration, reads data from an API, then transforms it using PySpark, loads it into the database and aggreagates it using SQL querys.