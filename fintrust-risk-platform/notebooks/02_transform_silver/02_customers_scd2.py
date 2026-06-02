# Databricks notebook source
import uuid
from datetime import datetime, timezone
from src.common.config import load_config, table_name
from src.common.cdc import merge_type2
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
source_table = table_name(config, "bronze", "customers_raw")
target_table = table_name(config, "silver", "customers_scd2")
recon_table  = table_name(config, "audit", "reconciliation_results")
health_table = table_name(config, "audit", "pipeline_health")

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

try:
    stage_df = spark.sql(f"""
        SELECT
            customer_id,
            customer_name,
            customer_country,
            customer_risk_tier,
            kyc_status,
            CAST(effective_date AS DATE) AS effective_date
        FROM {source_table}
        WHERE customer_id IS NOT NULL
    """)

    merge_type2(
        spark=spark,
        source_df=stage_df,
        target_table=target_table,
        key_columns=["customer_id"],
        tracked_columns=["customer_name", "customer_country", "customer_risk_tier", "kyc_status"],
        effective_date_col="effective_date",
    )

    source_count = spark.sql(f"SELECT COUNT(*) FROM {source_table}").collect()[0][0]
    target_count = spark.sql(f"SELECT COUNT(*) FROM {target_table} WHERE is_current = true").collect()[0][0]

    write_reconciliation_result(spark, recon_table, "customers_scd2", "bronze_to_silver", source_count, target_count)
    write_pipeline_health(spark, health_table, "customers_scd2", "silver", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed=target_count)
except Exception as e:
    write_pipeline_health(spark, health_table, "customers_scd2", "silver", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Applied SCD2 merge from {source_table} into {target_table}")
