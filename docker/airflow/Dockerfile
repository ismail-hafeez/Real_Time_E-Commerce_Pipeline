# docker/airflow/Dockerfile
FROM apache/airflow:2.8.1

USER root

# Install system-level dependencies required for compiling Python packages (if any)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages required for the pipeline:
# 1. apache-airflow-providers-amazon: For S3 operators and S3 hooks
# 2. apache-airflow-providers-apache-spark: For SparkSubmitOperator to run Spark jobs
# 3. boto3: For custom Python S3 operations
# 4. psycopg2-binary: PostgreSQL adapter to write/query the warehouse
# 5. requests: To fetch daily currency data from the Frankfurter API
RUN pip install --no-cache-dir \
    apache-airflow-providers-amazon \
    apache-airflow-providers-apache-spark \
    boto3 \
    psycopg2-binary \
    requests