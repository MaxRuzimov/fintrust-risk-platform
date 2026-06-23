# Databricks notebook source
# 02_transform_silver/04_merchants_to_silver.py
#
# Thin wiring. Reads bronze merchants, runs the split funnel from
# src/silver/clean_merchants.py, writes clean -> silver, rejects -> quarantine.
# Batch dimension load: INSERT OVERWRITE semantics via overwrite mode so the
# table is an idempotent rebuild (re-running gives the same result, no residue).

import uuid
from datetime import datetime, timezone

from src.common.config import load_config, table_name
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health
from src.silver.clean_merchants import split_merchants, deduplicate

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
source_table     = table_name(config, "bronze", "merchants_raw")
silver_table     = table_name(config, "silver", "merchants")
quarantine_table = table_name(config, "quarantine", "invalid_merchants")
recon_table      = table_name(config, "audit", "reconciliation_results")
health_table     = table_name(config, "audit", "pipeline_health")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

try:
    bronze_df = spark.table(source_table)

    clean_df, reject_df = split_merchants(bronze_df)
    clean_df = deduplicate(clean_df)

    # idempotent rebuild of the dimension
    clean_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

    # quarantine is append-only (accumulates rejects over time)
    if reject_df.count() > 0:
        reject_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    source_count = bronze_df.count()
    target_count = spark.table(silver_table).count()

    write_reconciliation_result(spark, recon_table, "merchants_silver", "bronze_to_silver", source_count, target_count)
    write_pipeline_health(spark, health_table, "merchants_silver", "silver", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed=target_count)
except Exception as e:
    write_pipeline_health(spark, health_table, "merchants_silver", "silver", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Transformed merchants from {source_table} into {silver_table}")