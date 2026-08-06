import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Banking_Medallion_Silver_Cleansing") \
    .getOrCreate()

DELTA_PATH = "abfss://silver-container@bankingadls.dfs.core.windows.net/cleansed_transactions.parquet"

# Read from Bronze Table
bronze_df = spark.read.table("banking_catalog.bronze.raw_transactions")

# Read Customer Demographics CTE / Source Table
customers_df = spark.read.table("banking_catalog.bronze.raw_customers")

# Transformation & Cleansing
silver_df = bronze_df.filter(F.col("amount").isNotNull() & (F.col("amount") > 0)) \
    .join(customers_df, on="customer_id", how="inner") \
    .withColumn("is_high_value", F.when(F.col("amount") > 10000, 1).otherwise(0))

# Write to Silver Delta Lake
silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("path", DELTA_PATH) \
    .saveAsTable("banking_catalog.silver.cleansed_transactions")

# Spark SQL Transformation with Temp CTE Table and Set Operations
spark.sql("""
    WITH temp_valid_accounts AS (
        SELECT customer_id, account_id, balance
        FROM banking_catalog.silver.cleansed_transactions
        WHERE balance >= 0
    )
    CREATE TABLE IF NOT EXISTS banking_catalog.silver.verified_customer_accounts AS
    SELECT a.customer_id, a.account_id, c.kyc_status
    FROM temp_valid_accounts a
    INNER JOIN banking_catalog.bronze.raw_customers c ON a.customer_id = c.customer_id
    UNION
    SELECT customer_id, account_id, 'EXEMPT' as kyc_status
    FROM banking_catalog.bronze.raw_corporate_accounts;
""")
