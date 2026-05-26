"""
Airflow DAG to orchestrate Batch Processing Pipeline
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils import ingest_retail_data, fetch_and_upload_rates

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="batch_processing_pipeline",
    default_args=default_args,
    description="E-Commerce Batch Pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 5, 24),
    catchup=False,
    tags=["batch", "ecommerce", "spark"],
) as dag:

    # Task 1: Ingest Raw Data (XLSX -> Parquet -> S3)
    ingest_raw_data = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=ingest_retail_data,
        op_kwargs={'xlsx_path': '/opt/airflow/data/online_retail_II.xlsx'}
    )

    # Task 2: Fetch Exchange Rates (API -> Parquet -> S3)
    fetch_rates = PythonOperator(
        task_id='fetch_rates',
        python_callable=fetch_and_upload_rates,
        op_kwargs={'start_date': '2009-01-01', 'end_date': '2011-12-31'}
    )

    # Task 3: Spark Clean & Transform
    spark_transform = SparkSubmitOperator(
        task_id='spark_clean_transform',
        application='/opt/airflow/spark_jobs/batch/clean_transform.py',
        conn_id='spark_default',
        application_args=[os.getenv('S3_BUCKET_NAME', 'fallback-bucket')],
    )

    # Task 4: Spark Build Star Schema
    spark_build_dimensions = SparkSubmitOperator(
        task_id='spark_build_star_schema',
        application='/opt/airflow/spark_jobs/batch/build_dimensions.py',
        conn_id='spark_default',
        application_args=[os.getenv('S3_BUCKET_NAME', 'fallback-bucket')],
    )

    # Task Dependencies
    [ingest_raw_data, fetch_rates] >> spark_transform >> spark_build_dimensions