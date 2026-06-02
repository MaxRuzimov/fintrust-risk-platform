# Databricks notebook source
import uuid
from datetime import datetime, timezone
from src.common.config import load_config, table_name
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
source_table = table_name(config, "gold", "transactions_enriched")
target_table = table_name(config, "gold", "daily_transaction_summary")
recon_table  = table_name(config, "audit", "reconciliation_results")
health_table = table_name(config, "audit", "pipeline_health")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

try:
    spark.sql(f"""
    INSERT OVERWRITE {target_table}
    SELECT
        CAST(transaction_timestamp AS DATE) AS transaction_date,
        currency,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        SUM(amount_cad) AS total_amount_cad,
        current_timestamp() AS processed_at
    FROM {source_table}
    GROUP BY
        CAST(transaction_timestamp AS DATE),
        currency
    """)

    source_count = spark.sql(f"SELECT COUNT(DISTINCT CAST(transaction_timestamp AS DATE), currency) FROM {source_table}").collect()[0][0]
    target_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

    write_reconciliation_result(spark, recon_table, "daily_transaction_summary", "gold_aggregation", source_count, target_count)
    write_pipeline_health(spark, health_table, "daily_transaction_summary", "gold", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed=target_count)
except Exception as e:
    write_pipeline_health(spark, health_table, "daily_transaction_summary", "gold", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Daily transaction summary refreshed: {target_table}")
