# Databricks notebook source
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
transactions_enriched           = table_name(config, "gold", "transactions_enriched")
daily_transaction_summary       = table_name(config, "gold", "daily_transaction_summary")
fraud_risk_summary              = table_name(config, "gold", "fraud_risk_summary")
transactions_with_fraud_alerts  = table_name(config, "gold", "transactions_with_fraud_alerts")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {transactions_enriched} (
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
    rate_to_cad DOUBLE,
    amount_cad DOUBLE,
    gold_processed_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {daily_transaction_summary} (
    transaction_date DATE,
    currency STRING,
    transaction_count BIGINT,
    total_amount DOUBLE,
    total_amount_cad DOUBLE,
    processed_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {fraud_risk_summary} (
    transaction_date DATE,
    risk_level STRING,
    transaction_count BIGINT,
    total_amount_cad DOUBLE,
    processed_at TIMESTAMP
)
USING DELTA
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {transactions_with_fraud_alerts} (
    transaction_id STRING,
    customer_id STRING,
    merchant_id STRING,
    amount DOUBLE,
    currency STRING,
    transaction_timestamp TIMESTAMP,
    alert_id STRING,
    alert_type STRING,
    alert_severity STRING,
    alert_timestamp TIMESTAMP,
    processed_at TIMESTAMP
)
USING DELTA
""")

print(f"Gold tables created in catalog: {config['catalog']}")
