# Databricks notebook source
# 02_transform_silver/05_fraud_alerts_to_silver.py
#
# Thin wiring. Reads bronze fraud_alerts, runs the split funnel from
# src/silver/clean_fraud_alerts.py, writes clean -> silver, rejects -> quarantine.
# Batch feed: overwrite silver (idempotent rebuild), append quarantine.

import uuid
from datetime import datetime, timezone

from src.common.config import load_config, table_name
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health
from src.silver.clean_fraud_alerts import split_fraud_alerts, deduplicate

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
source_table     = table_name(config, "bronze", "fraud_alerts_raw")
silver_table     = table_name(config, "silver", "fraud_alerts")
quarantine_table = table_name(config, "quarantine", "invalid_fraud_alerts")
recon_table      = table_name(config, "audit", "reconciliation_results")
health_table     = table_name(config, "audit", "pipeline_health")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

try:
    bronze_df = spark.table(source_table)

    clean_df, reject_df = split_fraud_alerts(bronze_df)
    clean_df = deduplicate(clean_df)

    clean_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    if reject_df.count() > 0:
        reject_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    source_count = bronze_df.count()
    target_count = spark.table(silver_table).count()

    write_reconciliation_result(spark, recon_table, "fraud_alerts_silver", "bronze_to_silver", source_count, target_count)
    write_pipeline_health(spark, health_table, "fraud_alerts_silver", "silver", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed=target_count)
except Exception as e:
    write_pipeline_health(spark, health_table, "fraud_alerts_silver", "silver", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Transformed fraud_alerts from {source_table} into {silver_table}")