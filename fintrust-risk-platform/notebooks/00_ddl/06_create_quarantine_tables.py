# Databricks notebook source

catalog = "dbw_fintrust_platform_dev"
quarantine_schema = "quarantine"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{quarantine_schema}`.`invalid_transactions` (
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

print(f"Quarantine tables created in catalog: {catalog}")
