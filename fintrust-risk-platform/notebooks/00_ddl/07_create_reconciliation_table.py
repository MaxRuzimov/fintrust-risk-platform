# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
recon_table = table_name(config, "audit", "reconciliation_results")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {recon_table} (
    pipeline_name STRING,
    layer STRING,
    source_count BIGINT,
    target_count BIGINT,
    difference BIGINT,
    reconciliation_status STRING,
    validation_timestamp TIMESTAMP
)
USING DELTA
""")

print(f"Reconciliation table created: {recon_table}")
