# Databricks notebook source

from pyspark.sql.functions import current_timestamp, input_file_name, expr
from pyspark.sql.types import StructType, StructField, StringType

catalog = "dbw_fintrust_platform_dev"
landing_path = "/Volumes/dbw_fintrust_platform_dev/reference/landing/customers/"
checkpoint = "/Volumes/dbw_fintrust_platform_dev/audit/checkpoints/customers_bronze_autoloader"
target_table = f"`{catalog}`.`bronze`.`customers_raw`"

schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_country", StringType()),
    StructField("customer_risk_tier", StringType()),
    StructField("kyc_status", StringType()),
    StructField("effective_date", StringType()),
])

customers_df = (
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
    customers_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(target_table)
)

print(f"Loaded customers files from {landing_path} into {target_table}")
