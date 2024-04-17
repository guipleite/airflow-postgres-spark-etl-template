FROM apache/airflow:2.8.3
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
            vim \
    && apt install default-jdk  -y \       
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# RUN  export JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
# RUN  export PATH=$PATH:$JAVA_HOME/bin
USER airflow
COPY .env /dags/.env
COPY requirements.txt .
RUN pip install -r requirements.txt
