# Databricks notebook source
import uuid
from datetime import datetime, timezone
from src.common.config import load_config, table_name
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
transactions_table   = table_name(config, "silver", "transactions")
exchange_rates_table = table_name(config, "silver", "exchange_rates")
target_table         = table_name(config, "gold", "transactions_enriched")
recon_table          = table_name(config, "audit", "reconciliation_results")
health_table         = table_name(config, "audit", "pipeline_health")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

try:
    spark.sql(f"""
    INSERT OVERWRITE {target_table}
    SELECT
        t.transaction_id,
        t.customer_id,
        t.card_id,
        t.merchant_id,
        t.amount,
        t.currency,
        t.transaction_timestamp,
        t.channel,
        t.location_country,
        t.transaction_status,
        t.ingestion_timestamp,
        er.rate_to_cad,
        t.amount * er.rate_to_cad AS amount_cad,
        current_timestamp() AS gold_processed_at
    FROM {transactions_table} t
    LEFT JOIN {exchange_rates_table} er
        ON t.currency = er.currency
       AND CAST(t.transaction_timestamp AS DATE) = er.rate_date
    """)

    source_count = spark.sql(f"SELECT COUNT(*) FROM {transactions_table}").collect()[0][0]
    target_count = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]

    write_reconciliation_result(spark, recon_table, "transactions_enriched", "silver_to_gold", source_count, target_count)
    write_pipeline_health(spark, health_table, "transactions_enriched", "gold", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed=target_count)
except Exception as e:
    write_pipeline_health(spark, health_table, "transactions_enriched", "gold", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Gold transactions enriched created: {target_table}")
