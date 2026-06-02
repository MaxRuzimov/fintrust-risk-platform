# Databricks notebook source
import uuid
from datetime import datetime, timezone
from src.common.config import load_config, table_name
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
source_table = table_name(config, "bronze", "exchange_rates_raw")
target_table = table_name(config, "silver", "exchange_rates")
recon_table  = table_name(config, "audit", "reconciliation_results")
health_table = table_name(config, "audit", "pipeline_health")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

try:
    spark.sql(f"""
    INSERT OVERWRITE {target_table}
    SELECT
        UPPER(TRIM(currency)) AS currency,
        CAST(rate_date AS DATE) AS rate_date,
        CAST(rate_to_cad AS DOUBLE) AS rate_to_cad,
        current_timestamp() AS loaded_at
    FROM {source_table}
    WHERE currency IS NOT NULL
      AND rate_date IS NOT NULL
      AND rate_to_cad IS NOT NULL
    """)

    source_count = spark.sql(f"SELECT COUNT(*) FROM {source_table} WHERE currency IS NOT NULL AND rate_date IS NOT NULL AND rate_to_cad IS NOT NULL").collect()[0][0]
    target_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

    write_reconciliation_result(spark, recon_table, "exchange_rates_silver", "bronze_to_silver", source_count, target_count)
    write_pipeline_health(spark, health_table, "exchange_rates_silver", "silver", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed=target_count)
except Exception as e:
    write_pipeline_health(spark, health_table, "exchange_rates_silver", "silver", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Transformed exchange rates from {source_table} into {target_table}")
