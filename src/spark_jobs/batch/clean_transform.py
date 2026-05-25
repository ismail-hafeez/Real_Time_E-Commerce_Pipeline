"""
1- Reads raw/online_retail.parquet + raw/exchange_rates.parquet from S3
2- Cleaning: drop rows with null Customer ID, remove bad stock codes (POST, D, M, etc.)
3- Flag cancellations: invoices starting with C → is_cancelled = true
4- Join exchange rates on date → compute unit_price_usd and total_amount_usd
5- Writes to s3://.../processed/cleaned_orders.parquet
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, round

def main():
    if len(sys.argv) != 2:
        print("Usage: clean_transform.py <s3_bucket_name>")
        sys.exit(1)
        
    s3_bucket = sys.argv[1]
    
    # Initialize SparkSession with S3 integration
    spark = SparkSession.builder \
        .appName("Clean and Transform E-Commerce Data") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")

    raw_orders_path = f"s3a://{s3_bucket}/raw/online_retail.parquet"
    raw_rates_path = f"s3a://{s3_bucket}/raw/exchange_rates.parquet"
    output_path = f"s3a://{s3_bucket}/processed/cleaned_orders.parquet"

    print(f"Reading raw orders from {raw_orders_path}")
    df_orders = spark.read.parquet(raw_orders_path)
    
    print(f"Reading raw rates from {raw_rates_path}")
    df_rates = spark.read.parquet(raw_rates_path)

    # 1. Clean Orders
    print("Cleaning orders...")
    # Drop rows without Customer ID
    df_clean = df_orders.filter(col("`Customer ID`").isNotNull())
    
    # Rename and cast Customer ID
    df_clean = df_clean.withColumnRenamed("Customer ID", "customer_id") \
                       .withColumn("customer_id", col("customer_id").cast("integer").cast("string"))
                       
    # Rename other columns to standard snake_case
    df_clean = df_clean.withColumnRenamed("Invoice", "invoice_no") \
                       .withColumnRenamed("StockCode", "stock_code") \
                       .withColumnRenamed("Description", "description") \
                       .withColumnRenamed("Quantity", "quantity") \
                       .withColumnRenamed("InvoiceDate", "invoice_date") \
                       .withColumnRenamed("Price", "unit_price_gbp") \
                       .withColumnRenamed("Country", "country")
                       
    # Remove bad stock codes (e.g., POST, D, M, BANK CHARGES)
    # Most real products are purely numbers or numbers with a single letter suffix (e.g. 85123A).
    # Bad codes are typically purely alphabetic.
    df_clean = df_clean.filter(~col("stock_code").rlike("^[a-zA-Z]+$"))

    # Flag cancellations (invoices starting with 'C')
    df_clean = df_clean.withColumn("is_cancelled", col("invoice_no").startswith("C"))
    
    # Calculate Total Amount in GBP
    df_clean = df_clean.withColumn("total_amount_gbp", col("quantity") * col("unit_price_gbp"))
    
    # 2. Join with Exchange Rates
    print("Joining with exchange rates...")
    # Create a simple date column for joining
    df_clean = df_clean.withColumn("join_date", to_date(col("invoice_date")))
    df_rates = df_rates.withColumn("join_date", to_date(col("date")))
    
    # Perform left join
    df_joined = df_clean.join(df_rates, on="join_date", how="left")
    
    # Compute USD amounts (if rate is null, assume 1.5 as fallback for historical context)
    df_joined = df_joined.withColumn("gbp_to_usd", when(col("gbp_to_usd").isNull(), 1.5).otherwise(col("gbp_to_usd")))
    
    df_joined = df_joined.withColumn("unit_price_usd", round(col("unit_price_gbp") * col("gbp_to_usd"), 2)) \
                         .withColumn("total_amount_usd", round(col("total_amount_gbp") * col("gbp_to_usd"), 2))
                         
    # Drop temporary columns
    df_joined = df_joined.drop("join_date", "date")
    
    # 3. Write Processed Data
    print(f"Writing processed data to {output_path}")
    df_joined.write.mode("overwrite").parquet(output_path)
    
    print("Spark clean & transform complete!")
    spark.stop()

if __name__ == "__main__":
    main()
