# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import col, current_timestamp, input_file_name, lit, to_timestamp, when
from pyspark.sql.types import StructType, StructField, StringType
from src.common.config import load_config, table_name, volume_path, checkpoint_path
from src.common.contracts import FRAUD_ALERTS_CONTRACT
from src.common.contract_validator import validate_contract
from src.common.quality import split_valid_invalid_records, add_domain_validation_errors, split_domain_valid_invalid
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
landing_path     = volume_path(config, "landing_base_path", "fraud_alerts")
checkpoint       = checkpoint_path(config, "fraud_alerts_bronze_stream_v2")
target_table     = table_name(config, "bronze", "fraud_alerts_raw")
quarantine_table = table_name(config, "quarantine", "invalid_fraud_alerts")
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


def process_batch(batch_df, batch_id):
    total = batch_df.count()
    print(f"[Batch {batch_id}] Raw records received: {total}")
    if total == 0:
        return

    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    records_processed = 0
    records_quarantined = 0
    try:
        batch_df = batch_df.withColumn("alert_timestamp", to_timestamp(col("alert_timestamp")))
        print(f"[Batch {batch_id}] After to_timestamp: {batch_df.count()}")

        validate_contract(batch_df, FRAUD_ALERTS_CONTRACT, strict=False)

        valid_df, invalid_df = split_valid_invalid_records(batch_df, required_columns)
        print(f"[Batch {batch_id}] valid={valid_df.count()} invalid={invalid_df.count()}")

        valid_df = add_domain_validation_errors(valid_df, domain_rules)
        domain_valid_df, domain_invalid_df = split_domain_valid_invalid(valid_df)
        print(f"[Batch {batch_id}] domain_valid={domain_valid_df.count()} domain_invalid={domain_invalid_df.count()}")

        records_processed = domain_valid_df.count()
        domain_valid_df.write.format("delta").mode("append").saveAsTable(target_table)

        all_invalid_df = invalid_df.unionByName(domain_invalid_df, allowMissingColumns=True)
        records_quarantined = all_invalid_df.count()
        if records_quarantined > 0:
            (
                all_invalid_df
                .withColumn("alert_timestamp", col("alert_timestamp").cast("string"))
                .withColumn("quarantined_at", current_timestamp())
                .write.format("delta").mode("append").saveAsTable(quarantine_table)
            )

        print(f"[Batch {batch_id}] processed={records_processed} quarantined={records_quarantined}")
        write_pipeline_health(spark, health_table, "fraud_alerts_stream", "bronze", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed, records_quarantined)
    except Exception as e:
        print(f"[Batch {batch_id}] ERROR: {e}")
        write_pipeline_health(spark, health_table, "fraud_alerts_stream", "bronze", run_id, "FAILED", start_time, datetime.now(timezone.utc), records_processed, records_quarantined, str(e))
        raise


query = (
    fraud_alerts_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="30 seconds")
    .start()
)

print(f"Target Table    : {target_table}")
print(f"Quarantine Table: {quarantine_table}")
print(f"Checkpoint      : {checkpoint}")
print(f"Landing Path    : {landing_path}")

query.awaitTermination()
