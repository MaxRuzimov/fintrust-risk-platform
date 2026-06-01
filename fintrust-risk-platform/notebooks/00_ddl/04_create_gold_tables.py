# Databricks notebook source

catalog = "dbw_fintrust_platform_dev"
gold_schema = "gold"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{gold_schema}`.`transactions_enriched` (
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
CREATE TABLE IF NOT EXISTS `{catalog}`.`{gold_schema}`.`daily_transaction_summary` (
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
CREATE TABLE IF NOT EXISTS `{catalog}`.`{gold_schema}`.`fraud_risk_summary` (
    transaction_date DATE,
    risk_level STRING,
    transaction_count BIGINT,
    total_amount_cad DOUBLE,
    processed_at TIMESTAMP
)
USING DELTA
""")

print(f"Gold tables created in catalog: {catalog}")
