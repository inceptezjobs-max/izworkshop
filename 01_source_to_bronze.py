import os
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Banking_Medallion_Bronze_Ingestion") \
    .getOrCreate()

# 1. Config & Parameters
CONFIG_PATH = "s3://banking-lakehouse-prod/config/pipeline_config.json"
FILE_LOCATION = "s3://banking-raw-landing-zone/daily_transactions/raw_transactions.csv"

# 2. UDF Registration
@F.udf(returnType=StringType())
def mask_account_number(acc_num):
    if not acc_num or len(acc_num) < 4:
        return "****"
    return "XXXX" + str(acc_num)[-4:]

spark.udf.register("udf_mask_account", mask_account_number)

# 3. Read Raw Source File
raw_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(FILE_LOCATION)

# 4. Ingest into Bronze Delta Table (Raw Storage)
bronze_df = raw_df.withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("masked_account", mask_account_number(F.col("account_number")))

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("banking_catalog.bronze.raw_transactions")

# 5. Direct Spark SQL Execution
spark.sql("""
    CREATE TABLE IF NOT EXISTS banking_catalog.bronze.stg_raw_deposits AS
    SELECT 
        txn_id,
        customer_id,
        amount,
        txn_type,
        current_timestamp() as created_at
    FROM banking_catalog.bronze.raw_transactions
    WHERE txn_type = 'DEPOSIT';
""")
