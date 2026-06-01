# Databricks notebook source

catalog = "dbw_fintrust_platform_dev"
bronze_schema = "bronze"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS `{catalog}`.`{bronze_schema}`.`transactions_raw` (
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
CREATE TABLE IF NOT EXISTS `{catalog}`.`{bronze_schema}`.`customers_raw` (
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
CREATE TABLE IF NOT EXISTS `{catalog}`.`{bronze_schema}`.`exchange_rates_raw` (
    currency STRING,
    rate_date STRING,
    rate_to_cad STRING,
    source_file STRING,
    ingestion_timestamp TIMESTAMP,
    load_id STRING
)
USING DELTA
""")

print(f"Bronze tables created in catalog: {catalog}")
