FROM apache/airflow:2.8.3
COPY requirements.txt .
RUN pip install -r requirements.txt