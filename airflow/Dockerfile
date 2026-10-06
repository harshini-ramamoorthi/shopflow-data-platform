FROM apache/airflow:3.1.7

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:/home/airflow/shopflow-venv/bin:${PATH}"

USER airflow

# dbt dependencies

RUN python -m venv --system-site-packages /home/airflow/shopflow-venv \
    && /home/airflow/shopflow-venv/bin/pip install --no-cache-dir \
        pyspark==4.2.0 \
        SQLAlchemy==2.0.46 \
        psycopg2-binary==2.9.11 \
        python-dotenv==1.2.1 \
        dbt-core==1.12.5 \
        dbt-duckdb==1.11.0

        # pyspark dependencies

# RUN python -m venv --system-site-packages /home/airflow/shopflow-venv \
#     && /home/airflow/shopflow-venv/bin/pip install --no-cache-dir \
#         pyspark==4.2.0 \
#         SQLAlchemy==2.0.46 \
#         psycopg2-binary==2.9.11 \
#         python-dotenv==1.2.1
