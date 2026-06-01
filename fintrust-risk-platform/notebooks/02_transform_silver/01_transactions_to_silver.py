# Databricks notebook source

from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    upper,
    trim
)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

catalog      = "dbw_fintrust_platform_dev"
source_table = f"`{catalog}`.`bronze`.`transactions_raw`"
target_table = f"`{catalog}`.`silver`.`transactions`"
checkpoint   = "/Volumes/dbw_fintrust_platform_dev/audit/checkpoints/silver_transactions"

# ------------------------------------------------------------------
# Read Bronze Stream
# ------------------------------------------------------------------

bronze_df = (
    spark.readStream
    .table(source_table)
)

# ------------------------------------------------------------------
# Clean and Standardize
# ------------------------------------------------------------------

clean_df = (
    bronze_df
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("channel", upper(trim(col("channel"))))
    .withColumn("transaction_status", upper(trim(col("transaction_status"))))
    .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
)

# ------------------------------------------------------------------
# Valid Records
# ------------------------------------------------------------------

valid_df = (
    clean_df
    .filter(col("transaction_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("merchant_id").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("currency").isin("CAD", "USD", "EUR", "GBP"))
    .filter(col("transaction_timestamp").isNotNull())
)

# ------------------------------------------------------------------
# Deduplicate and Write Silver
# ------------------------------------------------------------------

silver_df = (
    valid_df
    .withWatermark("transaction_timestamp", "30 minutes")
    .dropDuplicates(["transaction_id"])
    .withColumn("silver_processed_at", current_timestamp())
)

query = (
    silver_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(target_table)
)

print(f"Source Table : {source_table}")
print(f"Target Table : {target_table}")
print(f"Checkpoint   : {checkpoint}")