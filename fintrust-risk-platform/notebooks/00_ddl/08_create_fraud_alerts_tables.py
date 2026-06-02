# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
fraud_alerts_raw    = table_name(config, "bronze", "fraud_alerts_raw")
invalid_fraud_alerts = table_name(config, "quarantine", "invalid_fraud_alerts")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fraud_alerts_raw} (
    alert_id STRING,
    customer_id STRING,
    merchant_id STRING,
    alert_type STRING,
    alert_severity STRING,
    alert_timestamp TIMESTAMP,
    source_file STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {invalid_fraud_alerts} (
    alert_id STRING,
    customer_id STRING,
    merchant_id STRING,
    alert_type STRING,
    alert_severity STRING,
    alert_timestamp STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    error_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA
""")

print(f"Fraud alerts tables created in catalog: {config['catalog']}")
