# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
transactions_raw   = table_name(config, "bronze", "transactions_raw")
customers_raw      = table_name(config, "bronze", "customers_raw")
exchange_rates_raw = table_name(config, "bronze", "exchange_rates_raw")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {transactions_raw} (
    transaction_id STRING,
    customer_id STRING,
    card_id STRING,
    merchant_id STRING,
    amount DOUBLE,
    currency STRING,
    transaction_timestamp STRING,
    channel STRING,
    location_country STRING,
    transaction_status STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {customers_raw} (
    customer_id STRING,
    customer_name STRING,
    customer_country STRING,
    customer_risk_tier STRING,
    kyc_status STRING,
    effective_date STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    load_id STRING
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {exchange_rates_raw} (
    currency STRING,
    rate_date STRING,
    rate_to_cad STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    load_id STRING
)
USING DELTA
""")

print(f"Bronze tables created in catalog: {config['catalog']}")
