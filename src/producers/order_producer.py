"""
Kafka Order Producer
Reads raw retail transactions from S3 and replays them as real-time events
into the 'orders.raw' Kafka topic, simulating a live e-commerce order feed.
Modes:
  - "test": 100 rows at 5 rows/sec  (~20 seconds, for pipeline verification)
  - "full": all rows at 200 rows/sec (~50 minutes, for full replay)
"""

import os
import io
import sys
import json
import time
import boto3
import pandas as pd
from kafka import KafkaProducer

# CONFIGURATION (from environment variables) 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "orders.raw")
PRODUCER_MODE = os.getenv("PRODUCER_MODE", "test")        # "test" or "full"
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Mode settings
MODE_CONFIG = {
    "test": {"max_rows": 100, "rows_per_second": 5},
    "full": {"max_rows": None, "rows_per_second": 200},
}

def download_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """
    Downloads a Parquet file from S3 and returns it as a Pandas DataFrame.
    """
    s3_client = boto3.client('s3')
    buffer = io.BytesIO()

    s3_client.download_fileobj(bucket, key, buffer)
    # Seek to the beginning of the buffer before reading
    buffer.seek(0)  
    df = pd.read_parquet(buffer)

    print(f"Loaded {len(df)} rows from s3://{bucket}/{key}")
    return df  

def create_kafka_producer() -> KafkaProducer:
    """
    Creates and returns a KafkaProducer instance configured for JSON serialization.
    """
    # Create the KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        retries=3
    )

    return producer

def row_to_event(row: pd.Series) -> dict:
    """
    Converts a single Pandas row into a JSON-serializable dictionary (Kafka message).
    """
    event = {
        "invoice_no": row["Invoice"],
        "stock_code": row["StockCode"],
        "description": row["Description"] if pd.notna(row["Description"]) else "",
        "quantity": int(row["Quantity"]),
        "invoice_date": pd.Timestamp(row["InvoiceDate"]).isoformat(),
        "unit_price": float(row["Price"]),
        "customer_id": str(row["Customer ID"]) if pd.notna(row["Customer ID"]) else "",
        "country": row["Country"]
    }
    return event

def produce_events(producer: KafkaProducer, df: pd.DataFrame, max_rows: int, rows_per_second: int) -> None:
    """
    Sends DataFrame rows to Kafka at the configured rate.
    """
    # Calculate sleep interval
    sleep_interval = 1 / rows_per_second

    if max_rows is None:
        max_rows = len(df)

    sent = 0
    # Sending events to Kafka 
    for _, row in df.iterrows():
        # Converting row -> event dict
        event = row_to_event(row)
        # Send event to Kafka
        producer.send(KAFKA_TOPIC, value=event) 
        sent += 1

        # Print progress every 50 rows
        if sent % 50 == 0 or sent == max_rows:
            print(f"Sent {sent} / {max_rows} events...")

        # Stop if we've sent the max number of rows    
        if sent >= max_rows:
            break
        
        # Sleep to control the rate
        time.sleep(sleep_interval) 
   
    # Ensure all messages are sent
    producer.flush()
    print(f"Done! {sent} events delivered to '{KAFKA_TOPIC}'")

def main():
    """Main entry point — orchestrates the full producer workflow."""
    
    print("=" * 60)
    print(f"  Kafka Order Producer")
    print(f"  Mode: {PRODUCER_MODE}")
    print(f"  Topic: {KAFKA_TOPIC}")
    print(f"  S3 Bucket: {S3_BUCKET_NAME}")
    print("=" * 60)

    # Validate config
    if not S3_BUCKET_NAME:
        print("ERROR: S3_BUCKET_NAME environment variable is not set!")
        sys.exit(1)
    
    config = MODE_CONFIG.get(PRODUCER_MODE)
    if not config:
        print(f"ERROR: Invalid PRODUCER_MODE '{PRODUCER_MODE}'. Use 'test' or 'full'.")
        sys.exit(1)

    # Step 1: Download raw data from S3
    s3_key = "raw/online_retail.parquet"
    print(f"\n[Step 1] Downloading s3://{S3_BUCKET_NAME}/{s3_key} ...")
    df = download_from_s3(S3_BUCKET_NAME, s3_key)
    
    # Step 2: Create Kafka producer
    print(f"\n[Step 2] Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    producer = create_kafka_producer()
    
    # Step 3: Stream events to Kafka
    print(f"\n[Step 3] Streaming events to topic '{KAFKA_TOPIC}' ...")
    produce_events(
        producer=producer,
        df=df,
        max_rows=config["max_rows"],
        rows_per_second=config["rows_per_second"]
    )
    
    print("\nProducer finished successfully!")

if __name__ == "__main__":
    main()
