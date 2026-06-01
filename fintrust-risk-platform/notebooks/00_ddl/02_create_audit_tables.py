# Databricks notebook source

catalog = "dbw_fintrust_platform_dev"
audit_schema = "audit"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{audit_schema}`.`batch_load_history` (
    load_id STRING,
    pipeline_name STRING,
    source_name STRING,
    source_file STRING,
    target_table STRING,
    record_count BIGINT,
    status STRING,
    error_message STRING,
    load_timestamp TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{audit_schema}`.`reconciliation_results` (
    check_id STRING,
    pipeline_name STRING,
    source_name STRING,
    source_count BIGINT,
    target_count BIGINT,
    difference BIGINT,
    status STRING,
    check_timestamp TIMESTAMP
)
USING DELTA
""")

print(f"Audit tables created in catalog: {catalog}")
