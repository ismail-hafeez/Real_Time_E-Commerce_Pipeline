"""
1. Read from Kafka: Subscribe to orders.raw topic, parse JSON messages into a Spark DataFrame
2. Parse & Cast: Cast invoice_date string → TimestampType, quantity → IntegerType, unit_price → DoubleType
3. Currency Enrichment: Fetch latest GBP→USD rate from Frankfurter API once at startup, broadcast to all workers, 
    multiply unit_price by rate to compute unit_price_usd
4. Anomaly Flagging: Flag rows where quantity < 0 (returns/cancellations) or unit_price * quantity > 5000 
    (unusually large orders) as is_anomaly = True
5. Sink 1 — S3 (append): Write enriched events to s3a://.../processed/streaming/ as Parquet using foreachBatch, partitioned by date
6. Sink 2 — Windowed Aggregation → PostgreSQL: Compute 5-minute tumbling window aggregations 
    (order_count, total_revenue_gbp, total_revenue_usd, avg_order_value, top_product_code, top_country) 
    and upsert into the realtime_metrics PostgreSQL table
"""

import os
import sys
import requests
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Configuration from Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

DB_HOST = os.getenv("DB_WAREHOUSE_HOST")
DB_PORT = os.getenv("DB_WAREHOUSE_PORT")
DB_NAME = os.getenv("DB_WAREHOUSE_NAME")
DB_USER = os.getenv("DB_WAREHOUSE_USER")
DB_PASSWORD = os.getenv("DB_WAREHOUSE_PASSWORD")
DB_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

def fetch_gbp_to_usd_rate() -> float:
    """
    Step 3 (Helper): Fetch latest GBP to USD rate from a free currency API.
    Provides a fallback rate if the API request fails.
    """
    url = "https://api.frankfurter.dev/v1/latest?base=GBP&symbols=USD"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            rate = data.get("rates", {}).get("USD", 1.27)
            print(f"[Currency API] Successfully fetched live GBP->USD rate: {rate}")
            return float(rate)
    except Exception as e:
        print(f"[Currency API] Warning: Failed to fetch currency rates ({e}). Using fallback rate.")
    
    # Fallback rate
    return 1.27

def create_spark_session() -> SparkSession:
    """
    Initializes SparkSession with support for Kafka and PostgreSQL JDBC driver.
    """
    # TODO: Add package requirements for spark-sql-kafka-0-10 and postgresql JDBC connector if needed
    return SparkSession.builder \
        .appName("RealTimeOrderStream") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

def get_orders_schema() -> StructType:
    """
    Defines and returns the Spark schema matching the Kafka message structure.
    """
    return StructType([
        StructField("invoice_no", StringType(), True),
        StructField("stock_code", StringType(), True),
        StructField("description", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("invoice_date", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("customer_id", StringType(), True),
        StructField("country", StringType(), True)
    ])

def process_batch(batch_df, batch_id):
    """
    Step 5 & 6: ForeachBatch function to write to S3 Parquet append and 
    PostgreSQL analytical tables.
    """
    # Ensure dataframe is cached if utilized in multiple targets
    batch_df.cache()

    try:
        # ------------------------------------------------------------
        # Step 5: Sink 1 — Write raw enriched events to S3 as Parquet
        # ------------------------------------------------------------
        # TODO: Define the target S3 path. Example: s3a://{S3_BUCKET_NAME}/processed/streaming/
        # TODO: Format/extract date from invoice_date for partitionBy("date")
        # Write batch_df to Parquet in append mode partitioned by date
        print(f"[Batch {batch_id}] Writing enriched records to S3...")
        
        # ------------------------------------------------------------
        # Step 6: Sink 2 — Tumbling Window Metrics & PostgreSQL Upsert
        # ------------------------------------------------------------
        # TODO: Compute 5-minute tumbling windows:
        # Group by: window(col("invoice_date"), "5 minutes")
        # Aggregate:
        #   - order_count: countDistinct("invoice_no")
        #   - total_revenue_gbp: sum(quantity * unit_price)
        #   - total_revenue_usd: sum(quantity * unit_price_usd)
        #   - avg_order_value: avg(quantity * unit_price)
        #   - top_product_code: custom aggregation or rank to find highest quantity stock code
        #   - top_country: custom aggregation or rank to find country with highest order count
        print(f"[Batch {batch_id}] Performing 5-minute tumbling aggregations...")
        
        # TODO: Upsert the aggregated metrics into PostgreSQL table 'realtime_metrics'
        # Tip: PostgreSQL uses JDBC driver. Use batch_df.write.format("jdbc").options(...).mode("append").save()
        # and handle upserts using a temporary staging table or custom SQL queries.
        print(f"[Batch {batch_id}] Upserting metrics to PostgreSQL table 'realtime_metrics'...")

    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")
    finally:
        # Uncache batch dataframe to release memory
        batch_df.unpersist()

def main():
    # 1. Fetch live currency conversion rate (GBP -> USD) once at startup
    gbp_usd_rate = fetch_gbp_to_usd_rate()
    
    # 2. Initialize Spark Session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Broadcast the rate to all worker nodes
    rate_broadcast = spark.sparkContext.broadcast(gbp_usd_rate)
    
    # 3. Read raw messages from Kafka topic
    print(f"Subscribing to Kafka topic '{KAFKA_TOPIC}' at {KAFKA_BOOTSTRAP_SERVERS}...")
    # TODO: Read streaming from Kafka source
    kafka_raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .load()
    
    # 4. Parse the raw JSON payload matching our schema
    orders_schema = get_orders_schema()
    
    # TODO: Parse value from bytes to string -> JSON columns
    # Example: F.from_json(F.col("value").cast("string"), orders_schema)
    parsed_stream = kafka_raw_stream \
        .select(F.col("value").cast("string").alias("json_payload")) \
        .select(F.from_json(F.col("json_payload"), orders_schema).alias("data")) \
        .select("data.*")
    
    # 5. Step 2 & 3 & 4: Transformations
    # TODO: Cast invoice_date string to TimestampType
    # TODO: Compute unit_price_usd using the broadcasted rate: F.col("unit_price") * F.lit(rate_broadcast.value)
    # TODO: Add 'is_anomaly' flag logic (quantity < 0 OR (quantity * unit_price) > 5000)
    enriched_stream = parsed_stream \
        .withColumn("invoice_date", F.to_timestamp(F.col("invoice_date"), "yyyy-MM-dd'T'HH:mm:ss")) \
        .withColumn("unit_price_usd", F.round(F.col("unit_price") * F.lit(rate_broadcast.value), 2)) \
        .withColumn(
            "is_anomaly",
            F.when((F.col("quantity") < 0) | ((F.col("quantity") * F.col("unit_price")) > 5000), True).otherwise(False)
        )
    
    # 6. Step 5 & 6: Sink using foreachBatch
    # TODO: Define checkpoint directory inside your S3 bucket or local path
    checkpoint_dir = "s3a://ecommerce-pipeline/checkpoints/streaming_order_job/" if S3_BUCKET_NAME else "./data/checkpoints/"
    
    print("Starting Spark Structured Streaming Query...")
    query = enriched_stream.writeStream \
        .foreachBatch(process_batch) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
