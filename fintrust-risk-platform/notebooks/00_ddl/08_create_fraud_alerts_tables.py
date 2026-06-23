# Databricks notebook source
# 00_ddl/10_create_fraud_alerts_tables.py
#
# All fraud_alerts tables in one place: bronze (raw), silver (clean feed for
# gold), quarantine (rejects). Self-contained — run once to provision.
#
# Schema decisions (must match src/silver/clean_fraud_alerts.py and the
# dedicated bronze loader 01_loaders/04_fraud_alerts_autoloader_to_bronze.py):
#   - bronze: alert_timestamp already cast to TIMESTAMP by the loader; other
#     fields STRING; + source_file, ingestion_timestamp.
#   - silver: standardised casing on type/severity; deduped on alert_id.
#   - quarantine: alert_timestamp stays STRING (rejects keep raw types) +
#     error_reason + quarantined_at.

from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
fraud_alerts_raw     = table_name(config, "bronze", "fraud_alerts_raw")
fraud_alerts         = table_name(config, "silver", "fraud_alerts")
invalid_fraud_alerts = table_name(config, "quarantine", "invalid_fraud_alerts")

# ── BRONZE: raw landing (loader already casts alert_timestamp) ────────────────
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

# ── SILVER: clean alert feed for gold to join against ────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fraud_alerts} (
    alert_id STRING,
    customer_id STRING,
    merchant_id STRING,
    alert_type STRING,
    alert_severity STRING,
    alert_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    silver_processed_at TIMESTAMP
)
USING DELTA
""")

# ── QUARANTINE: rejects keep raw STRING timestamp + reason/time ───────────────
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

print(f"Fraud alert tables (bronze/silver/quarantine) created in catalog: {config['catalog']}")
