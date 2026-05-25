"""
Helper functions for Airflow DAGs in src/airflow_dags/dags.py
"""
import pandas as pd
import boto3
import io
import os
from dotenv import load_dotenv

load_dotenv()

# Helper funtion for a Task: ingest_data
def ingest_retail_data(xlsx_path: str) -> None:
    """
    Reads XLSX using Pandas, writes directly to Parquet in S3 `raw/`
    """
    print(f"Reading {xlsx_path}...")
    df = pd.read_excel(xlsx_path)

    print("Converting to Parquet...")
    parquet_buffer = io.BytesIO()
    # Write to buffer using pyarrow
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

    s3_bucket = os.getenv('S3_BUCKET_NAME')
    s3_key = 'raw/online_retail.parquet'
    print(f"Uploading to s3://{s3_bucket}/{s3_key}...")
    
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue()
    )
    print("Upload complete!")
