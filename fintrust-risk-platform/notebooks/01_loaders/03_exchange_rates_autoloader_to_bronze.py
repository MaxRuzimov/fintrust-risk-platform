# Databricks notebook source

from pyspark.sql.functions import current_timestamp, input_file_name, expr
from pyspark.sql.types import StructType, StructField, StringType

catalog = "dbw_fintrust_platform_dev"
landing_path = "/Volumes/dbw_fintrust_platform_dev/reference/landing/exchange_rates/"
checkpoint = "/Volumes/dbw_fintrust_platform_dev/audit/checkpoints/exchange_rates_bronze_autoloader"
target_table = f"`{catalog}`.`bronze`.`exchange_rates_raw`"

schema = StructType([
    StructField("currency", StringType()),
    StructField("rate_date", StringType()),
    StructField("rate_to_cad", StringType()),
])

exchange_rates_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
    .option("header", "true")
    .schema(schema)
    .load(landing_path)
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("load_id", expr("uuid()"))
)

(
    exchange_rates_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(target_table)
)

print(f"Loaded exchange rate files from {landing_path} into {target_table}")
