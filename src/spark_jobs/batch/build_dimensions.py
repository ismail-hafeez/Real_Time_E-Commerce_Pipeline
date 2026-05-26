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
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, to_date, round, row_number, lit
from pyspark.sql.functions import avg, min, max, countDistinct
from pyspark.sql.functions import dayofweek, year, month, quarter, date_format
from pyspark.sql.window import Window

def upload_to_s3(df: DataFrame, file_name: str, output_path: str) -> None:
    print(f"Writing processed data to {output_path}{file_name}")
    df.write.mode("overwrite").parquet(f'{output_path}{file_name}')

def _dim_customer(df_orders: DataFrame, output_path: str) -> None:

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

def _dim_country(df_orders: DataFrame, output_path: str) -> None:

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

def _dim_products(df_orders: DataFrame, output_path: str) -> None:

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

def _dim_date(df_orders: DataFrame, output_path: str) -> None:

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

    # Building Dimension Tables
    _dim_customer(df_orders, output_path)
    _dim_country(df_orders, output_path)
    _dim_products(df_orders, output_path)
    _dim_date(df_orders, output_path)
    
    print("Spark star schema build complete!")
    spark.stop()

if __name__ == "__main__":
    main()
