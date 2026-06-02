# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
batch_load_history = table_name(config, "audit", "batch_load_history")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {batch_load_history} (
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

print(f"Audit tables created in catalog: {config['catalog']}")
