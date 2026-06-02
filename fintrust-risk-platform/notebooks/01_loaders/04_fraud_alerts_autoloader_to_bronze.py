# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType
from src.common.config import load_config, table_name, volume_path, checkpoint_path
from src.common.contracts import FRAUD_ALERTS_CONTRACT
from src.common.contract_validator import validate_contract
from src.common.quality import split_valid_invalid_records, add_domain_validation_errors, split_domain_valid_invalid
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
landing_path     = volume_path(config, "landing_base_path", "fraud_alerts")
checkpoint       = checkpoint_path(config, "fraud_alerts_bronze_autoloader")
target_table     = table_name(config, "bronze", "fraud_alerts_raw")
quarantine_table = table_name(config, "quarantine", "invalid_fraud_alerts")
recon_table      = table_name(config, "audit", "reconciliation_results")
health_table     = table_name(config, "audit", "pipeline_health")

schema = StructType([
    StructField("alert_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("alert_type", StringType()),
    StructField("alert_severity", StringType()),
    StructField("alert_timestamp", StringType()),
])

required_columns = ["alert_id", "customer_id", "merchant_id", "alert_type", "alert_severity", "alert_timestamp"]

domain_rules = [
    when(~col("alert_severity").isin("LOW", "MEDIUM", "HIGH", "CRITICAL"), lit("INVALID_ALERT_SEVERITY")),
    when(col("alert_timestamp").isNull(), lit("INVALID_ALERT_TIMESTAMP")),
]

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

source_count      = spark.read.format("csv").option("header", "true").schema(schema).load(landing_path).count()
before_target     = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
before_quarantine = spark.sql(f"SELECT COUNT(*) FROM {quarantine_table}").collect()[0][0]

fraud_alerts_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
    .option("header", "true")
    .schema(schema)
    .load(landing_path)
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)


def process_batch(batch_df, _):
    batch_df = batch_df.withColumn("alert_timestamp", to_timestamp(col("alert_timestamp")))

    validate_contract(batch_df, FRAUD_ALERTS_CONTRACT, strict=False)

    valid_df, invalid_df = split_valid_invalid_records(batch_df, required_columns)

    valid_df = add_domain_validation_errors(valid_df, domain_rules)
    domain_valid_df, domain_invalid_df = split_domain_valid_invalid(valid_df)

    domain_valid_df.write.format("delta").mode("append").saveAsTable(target_table)

    all_invalid_df = invalid_df.unionByName(domain_invalid_df, allowMissingColumns=True)
    if all_invalid_df.count() > 0:
        (
            all_invalid_df
            .withColumn("quarantined_at", current_timestamp())
            .write.format("delta").mode("append").saveAsTable(quarantine_table)
        )


try:
    query = (
        fraud_alerts_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    target_count        = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
    quarantine_count    = spark.sql(f"SELECT COUNT(*) FROM {quarantine_table}").collect()[0][0]
    records_processed   = target_count - before_target
    records_quarantined = quarantine_count - before_quarantine

    write_reconciliation_result(spark, recon_table, "fraud_alerts_autoloader", "landing_to_bronze", source_count, records_processed + records_quarantined)
    write_pipeline_health(spark, health_table, "fraud_alerts_autoloader", "bronze", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed, records_quarantined)
except Exception as e:
    write_pipeline_health(spark, health_table, "fraud_alerts_autoloader", "bronze", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Loaded fraud alerts from {landing_path} into {target_table}")
print(f"Records processed  : {records_processed}")
print(f"Records quarantined: {records_quarantined}")
