# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
customers_scd2 = table_name(config, "silver", "customers_scd2")
exchange_rates = table_name(config, "silver", "exchange_rates")
transactions   = table_name(config, "silver", "transactions")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {customers_scd2} (
    customer_id STRING,
    customer_name STRING,
    customer_country STRING,
    customer_risk_tier STRING,
    kyc_status STRING,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {exchange_rates} (
    currency STRING,
    rate_date DATE,
    rate_to_cad DOUBLE,
    loaded_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {transactions} (
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
    silver_processed_at TIMESTAMP
)
USING DELTA
""")

print(f"Silver tables created in catalog: {config['catalog']}")
