"""
1- Reads processed/cleaned_orders.parquet from S3
2- Builds dimension tables:
    dim_customer — distinct customers with first/last purchase dates, order count
    dim_product — distinct stock codes with description, average price
    dim_date — calendar dimension for all dates in the dataset
    dim_country — distinct countries with a basic region mapping
3- Builds fact_orders — one row per invoice line, with foreign keys to all dimensions
4- Writes all tables to s3://.../curated/ as Parquet
5- Writes all tables to PostgreSQL via JDBC (truncate + insert)
"""

import sys
import os
import psycopg2

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, to_date, round, row_number, lit
from pyspark.sql.functions import min, max, countDistinct, avg
from pyspark.sql.functions import dayofweek, year, month, quarter, date_format
from pyspark.sql.window import Window

def upload_to_s3(df: DataFrame, file_name: str, output_path: str) -> None:
    print(f"Writing processed data to {output_path}{file_name}")
    df.write.mode("overwrite").parquet(f'{output_path}{file_name}')

def truncate_warehouse_tables() -> None:
    # Load database connection details dynamically from environment variables
    db_host = os.getenv("DB_WAREHOUSE_HOST", "postgres-warehouse")
    db_port = os.getenv("DB_WAREHOUSE_PORT", "5432")
    db_name = os.getenv("DB_WAREHOUSE_NAME", "ecom_warehouse")
    db_user = os.getenv("DB_WAREHOUSE_USER", "warehouse_user")
    db_pass = os.getenv("DB_WAREHOUSE_PASSWORD", "warehouse_password")
    
    print("Connecting to PostgreSQL to truncate tables with CASCADE...")
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_pass
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE fact_orders, dim_customer, dim_product, dim_date, dim_country CASCADE;")
    print("Analytical tables truncated successfully!")
    cursor.close()
    conn.close()

def write_to_postgres(df: DataFrame, table_name: str) -> None:
    # Load database connection details dynamically from environment variables
    db_host = os.getenv("DB_WAREHOUSE_HOST", "postgres-warehouse")
    db_port = os.getenv("DB_WAREHOUSE_PORT", "5432")
    db_name = os.getenv("DB_WAREHOUSE_NAME", "ecom_warehouse")
    db_user = os.getenv("DB_WAREHOUSE_USER", "warehouse_user")
    db_pass = os.getenv("DB_WAREHOUSE_PASSWORD", "warehouse_password")
    
    db_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    
    print(f"Writing {table_name} to PostgreSQL...")
    df.write \
      .format("jdbc") \
      .option("url", db_url) \
      .option("dbtable", table_name) \
      .option("user", db_user) \
      .option("password", db_pass) \
      .option("driver", "org.postgresql.Driver") \
      .mode("append") \
      .save()

def _dim_customer(df_orders: DataFrame, output_path: str) -> DataFrame:
    # Aggregate raw orders by customer
    dim_customer = df_orders.groupBy("customer_id").agg(
        max("country").alias("country"),
        min(to_date("invoice_date")).alias("first_purchase_date"),
        max(to_date("invoice_date")).alias("last_purchase_date"),
        countDistinct("invoice_no").alias("total_orders")
    )
    
    # Define a window ordered by customer_id
    windowSpec = Window.orderBy("customer_id")
    dim_customer = dim_customer.withColumn("customer_key", row_number().over(windowSpec)) \
                                .withColumn("rfm_segment", lit("Active")) # Placeholder for now
    
    upload_to_s3(dim_customer, 'dim_customer', output_path)
    write_to_postgres(dim_customer, 'dim_customer')
    return dim_customer

def _dim_country(df_orders: DataFrame, output_path: str) -> DataFrame:
    # Define our list of European countries
    europe_countries = [
        "EIRE", "Germany", "France", "Netherlands", "Spain", "Switzerland", 
        "Portugal", "Belgium", "Channel Islands", "Sweden", "Italy", "Cyprus", 
        "Austria", "Denmark", "Norway", "Finland", "Greece", "Poland", 
        "Malta", "Lithuania", "Iceland"
    ]

    # Build dimension
    dim_country = df_orders.select("country").distinct() \
        .withColumnRenamed("country", "country_name")
        
    # Map regions
    dim_country = dim_country.withColumn("region",
        when(col("country_name") == "United Kingdom", "UK")
        .when(col("country_name").isin(europe_countries), "Europe")
        .otherwise("Rest of World")
    )

    # Map currencies
    dim_country = dim_country.withColumn("currency_code",
        when(col("country_name") == "United Kingdom", "GBP")
        .when(col("country_name").isin(europe_countries), "EUR")
        .otherwise("USD")
    )
    
    # Define a window ordered by country_name
    windowSpec = Window.orderBy("country_name")
    dim_country = dim_country.withColumn("country_key", row_number().over(windowSpec)) 
    
    upload_to_s3(dim_country, 'dim_country', output_path)
    write_to_postgres(dim_country, 'dim_country')
    return dim_country

def _dim_products(df_orders: DataFrame, output_path: str) -> DataFrame:
    # Aggregate raw orders by stock_code
    dim_product = df_orders.groupBy("stock_code").agg(
        max("description").alias("description"),
        round(avg("unit_price_gbp"), 2).alias("avg_price_gbp")
    )

    # Define a window ordered by stock_code
    windowSpec = Window.orderBy("stock_code")
    dim_product = dim_product.withColumn("product_key", row_number().over(windowSpec)) \
                                .withColumn("category", lit("General")) # Placeholder for now
    
    upload_to_s3(dim_product, 'dim_product', output_path)
    write_to_postgres(dim_product, 'dim_product')
    return dim_product

def _dim_date(df_orders: DataFrame, output_path: str) -> DataFrame:
    # 1. Get unique date values from the transaction table
    dim_date = df_orders.select(to_date("invoice_date").alias("full_date")).distinct()

    # 2. Extract calendar elements
    dim_date = dim_date.withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("integer")) \
                    .withColumn("day_of_week", ((dayofweek(col("full_date")) + 5) % 7).cast("short")) \
                    .withColumn("day_name", date_format(col("full_date"), "EEEE")) \
                    .withColumn("month", month(col("full_date")).cast("short")) \
                    .withColumn("month_name", date_format(col("full_date"), "MMMM")) \
                    .withColumn("quarter", quarter(col("full_date")).cast("short")) \
                    .withColumn("year", year(col("full_date")).cast("short")) \
                    .withColumn("is_weekend", col("day_of_week").isin(5, 6))

    upload_to_s3(dim_date, 'dim_date', output_path)
    write_to_postgres(dim_date, 'dim_date')
    return dim_date

def _build_fact_orders(df_orders: DataFrame, df_cust: DataFrame, df_prod: DataFrame, df_coun: DataFrame, output_path: str) -> None:
    # Select only business keys and surrogate keys to prevent column naming ambiguity
    df_cust_keys = df_cust.select("customer_id", "customer_key")
    df_prod_keys = df_prod.select("stock_code", "product_key")
    df_coun_keys = df_coun.select("country_name", "country_key")

    # Perform inner joins to map surrogate keys
    df_fact = df_orders.join(df_cust_keys, on="customer_id", how="inner") \
                       .join(df_prod_keys, on="stock_code", how="inner") \
                       .join(df_coun_keys, df_orders["country"] == df_coun_keys["country_name"], how="inner")

    # Generate additional fields and the date_key via the direct format formula
    df_fact = df_fact.withColumn("date_key", date_format(to_date(col("invoice_date")), "yyyyMMdd").cast("integer")) \
                     .withColumn("exchange_rate", col("gbp_to_usd")) \
                     .withColumn("is_anomaly", lit(False)) \
                     .withColumn("event_timestamp", col("invoice_date"))

    # Generate the surrogate fact key (order_key)
    windowSpec = Window.orderBy("invoice_no", "stock_code")
    df_fact = df_fact.withColumn("order_key", row_number().over(windowSpec))

    # Select final columns to match fact_orders PostgreSQL schema
    fact_orders = df_fact.select(
        "order_key",
        "invoice_no",
        "customer_key",
        "product_key",
        "date_key",
        "country_key",
        "quantity",
        "unit_price_gbp",
        "unit_price_usd",
        "total_amount_gbp",
        "total_amount_usd",
        "exchange_rate",
        "is_cancelled",
        "is_anomaly",
        "event_timestamp"
    )

    upload_to_s3(fact_orders, 'fact_orders', output_path)
    write_to_postgres(fact_orders, 'fact_orders')

def main():
    if len(sys.argv) != 2:
        print("Usage: build_dimensions.py <s3_bucket_name>")
        sys.exit(1)
        
    s3_bucket = sys.argv[1]
    
    # Initialize SparkSession with S3 integration
    spark = SparkSession.builder \
        .appName("Build E-Commerce Star Schema") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    input_path = f"s3a://{s3_bucket}/processed/cleaned_orders.parquet"
    output_path = f"s3a://{s3_bucket}/curated/"

    print(f"Reading Cleaned orders from {input_path}")
    df_orders = spark.read.parquet(input_path)

    # Truncate warehouse tables first with CASCADE to handle foreign key dependencies
    truncate_warehouse_tables()

    # Building Tables (In-Memory Pipeline)
    df_cust = _dim_customer(df_orders, output_path)
    df_coun = _dim_country(df_orders, output_path)
    df_prod = _dim_products(df_orders, output_path)
    _dim_date(df_orders, output_path)
    
    print("Building Fact Table...")
    _build_fact_orders(df_orders, df_cust, df_prod, df_coun, output_path)
    
    print("Spark star schema build complete!")
    spark.stop()

if __name__ == "__main__":
    main()
