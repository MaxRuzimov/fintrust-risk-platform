# Databricks notebook source
# 00_ddl/11_create_gold_risk_tables.py
#
# The two cross-source gold risk tables. Schemas match the gold modules
# src/gold/build_customer_risk_360.py and build_merchant_risk_summary.py.

from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
customer_risk_360     = table_name(config, "gold", "customer_risk_360")
merchant_risk_summary = table_name(config, "gold", "merchant_risk_summary")

# ── customer_risk_360 : one row per current customer ─────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {customer_risk_360} (
    customer_id STRING,
    customer_name STRING,
    customer_country STRING,
    customer_risk_tier STRING,
    kyc_status STRING,
    transaction_count BIGINT,
    total_amount DOUBLE,
    distinct_merchants BIGINT,
    alert_count BIGINT,
    max_alert_severity_rank INT,
    overall_risk_flag STRING,
    processed_at TIMESTAMP
)
USING DELTA
""")

# ── merchant_risk_summary : one row per merchant ─────────────────────────────
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {merchant_risk_summary} (
    merchant_id STRING,
    merchant_name STRING,
    merchant_category STRING,
    merchant_country STRING,
    risk_category STRING,
    is_high_risk_merchant BOOLEAN,
    transaction_count BIGINT,
    total_amount DOUBLE,
    distinct_customers BIGINT,
    alert_count BIGINT,
    overall_risk_flag STRING,
    processed_at TIMESTAMP
)
USING DELTA
""")

print(f"Gold risk tables created in catalog: {config['catalog']}")
