# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, to_timestamp, upper, trim
from src.common.config import load_config, table_name, checkpoint_path

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
source_table = table_name(config, "bronze", "transactions_raw")
target_table = table_name(config, "silver", "transactions")
checkpoint   = checkpoint_path(config, "silver_transactions")

bronze_df = spark.readStream.table(source_table)

clean_df = (
    bronze_df
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("channel", upper(trim(col("channel"))))
    .withColumn("transaction_status", upper(trim(col("transaction_status"))))
    .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
)

valid_df = (
    clean_df
    .filter(col("transaction_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("merchant_id").isNotNull())
    .filter(col("amount") > 0)
    .filter(col("currency").isin("CAD", "USD", "EUR", "GBP"))
    .filter(col("transaction_timestamp").isNotNull())
)

silver_df = (
    valid_df
    .withWatermark("transaction_timestamp", "30 minutes")
    .dropDuplicates(["transaction_id"])
    .withColumn("silver_processed_at", current_timestamp())
)

query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(target_table)
)

print(f"Source Table : {source_table}")
print(f"Target Table : {target_table}")
print(f"Checkpoint   : {checkpoint}")
