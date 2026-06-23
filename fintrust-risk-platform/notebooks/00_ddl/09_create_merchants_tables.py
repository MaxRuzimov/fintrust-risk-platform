# Databricks notebook source
# 00_ddl/09_create_merchants_tables.py
#
# All merchant tables in one place: bronze (raw), silver (typed dimension),
# quarantine (rejects). Self-contained — run this once to provision merchants.
#
# Schema decisions (must match the silver module src/silver/clean_merchants.py):
#   - bronze: every column STRING (CSV loader reads all as string) + loader
#     metadata (source_file, ingestion_timestamp, load_id).
#   - silver: is_high_risk_merchant cast to BOOLEAN; standardised + deduped.
#   - quarantine: mirrors raw STRING columns (rejects are split before cleaning,
#     so they keep original types) + error_reason + quarantined_at.

from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
merchants_raw     = table_name(config, "bronze", "merchants_raw")
merchants         = table_name(config, "silver", "merchants")
invalid_merchants = table_name(config, "quarantine", "invalid_merchants")

# ── BRONZE: raw landing (all strings + loader metadata) ──────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {merchants_raw} (
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    merchant_country STRING,
    risk_category STRING,
    is_high_risk_merchant STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    load_id STRING
)
USING DELTA
""")

# ── SILVER: typed, deduplicated dimension ────────────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {merchants} (
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    merchant_country STRING,
    risk_category STRING,
    is_high_risk_merchant BOOLEAN,
    ingestion_timestamp TIMESTAMP,
    silver_processed_at TIMESTAMP
)
USING DELTA
""")

# ── QUARANTINE: rejects keep raw STRING types + reason/time ──────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {invalid_merchants} (
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    merchant_country STRING,
    risk_category STRING,
    is_high_risk_merchant STRING,
    ingestion_timestamp TIMESTAMP,
    load_id STRING,
    error_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA
""")

print(f"Merchant tables (bronze/silver/quarantine) created in catalog: {config['catalog']}")
