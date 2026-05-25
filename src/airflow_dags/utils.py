"""
Helper functions for Airflow DAGs in src/airflow_dags/dags.py
"""
import pandas as pd
import boto3
import io
import os
import requests
from dotenv import load_dotenv

load_dotenv()

# Helper function
def upload_to_s3(df: pd.DataFrame, s3_key: str) -> None:

    print("Converting to Parquet...")
    parquet_buffer = io.BytesIO()
    # Write to buffer using pyarrow
    df.to_parquet(parquet_buffer, index=False, engine='pyarrow')

    s3_bucket = os.getenv('S3_BUCKET_NAME')
    print(f"Uploading to s3://{s3_bucket}/{s3_key}...")
    
    s3_client = boto3.client('s3')
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue()
    )
    print("Upload complete!")    

# Task 1: ingest_data
def ingest_retail_data(xlsx_path: str) -> None:
    """
    Reads XLSX using Pandas, writes directly to Parquet in S3 `raw/`
    """
    print(f"Reading {xlsx_path}...")
    df = pd.read_excel(xlsx_path)
    s3_key = 'raw/online_retail.parquet'

    upload_to_s3(df, s3_key)

# Task 2: fetch_rates
def fetch_and_upload_rates(start_date: str, end_date: str) -> None:
    """
    Fetches historical GBP to USD exchange rates from Frankfurter API,
    converts to Parquet, and uploads to S3 `raw/`
    """
    url = f"https://api.frankfurter.app/{start_date}..{end_date}?from=GBP&to=USD"
    print(f"Fetching exchange rates from {url}")
    
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    # Parse rates into a flat list
    rates_data = []
    for date_str, rates in data.get("rates", {}).items():
        rates_data.append({
            "date": date_str,
            "gbp_to_usd": rates.get("USD")
        })
        
    df = pd.DataFrame(rates_data)
    
    # Convert date string to datetime for easier joining in Spark
    df['date'] = pd.to_datetime(df['date'])
    s3_key = 'raw/exchange_rates.parquet'

    upload_to_s3(df, s3_key)
    

# Helper function for Task 3: