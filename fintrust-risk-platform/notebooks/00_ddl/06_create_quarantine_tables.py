# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
invalid_transactions = table_name(config, "quarantine", "invalid_transactions")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {invalid_transactions} (
    transaction_id STRING,
    customer_id STRING,
    card_id STRING,
    merchant_id STRING,
    amount DOUBLE,
    currency STRING,
    transaction_timestamp TIMESTAMP,
    channel STRING,
    location_country STRING,
    transaction_status STRING,
    ingestion_timestamp TIMESTAMP,
    error_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA
""")

invalid_customers = table_name(config, "quarantine", "invalid_customers")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {invalid_customers} (
    customer_id STRING,
    customer_name STRING,
    customer_country STRING,
    customer_risk_tier STRING,
    kyc_status STRING,
    effective_date STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    error_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA
""")

invalid_exchange_rates = table_name(config, "quarantine", "invalid_exchange_rates")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {invalid_exchange_rates} (
    currency STRING,
    rate_date STRING,
    rate_to_cad STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    load_id STRING,
    error_reason STRING,
    quarantined_at TIMESTAMP
)
USING DELTA
""")

print(f"Quarantine tables created in catalog: {config['catalog']}")
