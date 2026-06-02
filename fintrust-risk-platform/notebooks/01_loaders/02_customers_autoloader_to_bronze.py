# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import current_timestamp, input_file_name, expr
from pyspark.sql.types import StructType, StructField, StringType
from src.common.config import load_config, table_name, volume_path, checkpoint_path
from src.common.reconciliation import write_reconciliation_result
from src.common.quality import split_valid_invalid_records
from src.common.monitoring import write_pipeline_health
from src.common.contracts import CUSTOMERS_CONTRACT
from src.common.contract_validator import validate_contract

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
landing_path     = volume_path(config, "landing_base_path", "customers")
checkpoint       = checkpoint_path(config, config["sources"]["customers_file"]["checkpoint_name"])
target_table     = table_name(config, "bronze", "customers_raw")
quarantine_table = table_name(config, "quarantine", "invalid_customers")
recon_table      = table_name(config, "audit", "reconciliation_results")
health_table     = table_name(config, "audit", "pipeline_health")

schema = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_name", StringType()),
    StructField("customer_country", StringType()),
    StructField("customer_risk_tier", StringType()),
    StructField("kyc_status", StringType()),
    StructField("effective_date", StringType()),
])

required_columns = ["customer_id", "customer_name", "customer_country", "customer_risk_tier", "kyc_status", "effective_date"]

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)
source_count = spark.read.format("csv").option("header", "true").schema(schema).load(landing_path).count()

before_target     = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
before_quarantine = spark.sql(f"SELECT COUNT(*) FROM {quarantine_table}").collect()[0][0]

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


def process_batch(batch_df, _):
    validate_contract(batch_df, CUSTOMERS_CONTRACT, strict=False)
    valid_df, invalid_df = split_valid_invalid_records(batch_df, required_columns)
    valid_df.write.format("delta").mode("append").saveAsTable(target_table)
    if invalid_df.count() > 0:
        (
            invalid_df
            .withColumn("quarantined_at", current_timestamp())
            .write.format("delta").mode("append").saveAsTable(quarantine_table)
        )


try:
    query = (
        customers_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    target_count      = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
    quarantine_count  = spark.sql(f"SELECT COUNT(*) FROM {quarantine_table}").collect()[0][0]
    records_processed   = target_count - before_target
    records_quarantined = quarantine_count - before_quarantine

    write_reconciliation_result(spark, recon_table, "customers_autoloader", "landing_to_bronze", source_count, target_count)
    write_pipeline_health(spark, health_table, "customers_autoloader", "bronze", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed, records_quarantined)
except Exception as e:
    write_pipeline_health(spark, health_table, "customers_autoloader", "bronze", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Loaded customers files from {landing_path} into {target_table}")
