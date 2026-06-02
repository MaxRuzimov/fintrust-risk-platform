# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
pipeline_health = table_name(config, "audit", "pipeline_health")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {pipeline_health} (
    pipeline_name STRING,
    layer STRING,
    run_id STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    records_processed BIGINT,
    records_quarantined BIGINT,
    error_message STRING,
    created_at TIMESTAMP
)
USING DELTA
""")

print(f"Monitoring table created for environment: {env}")
