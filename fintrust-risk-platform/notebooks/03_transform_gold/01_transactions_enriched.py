# Databricks notebook source

catalog              = "dbw_fintrust_platform_dev"
transactions_table   = f"`{catalog}`.`silver`.`transactions`"
exchange_rates_table = f"`{catalog}`.`silver`.`exchange_rates`"
target_table         = f"`{catalog}`.`gold`.`transactions_enriched`"

spark.sql(f"""
INSERT OVERWRITE {target_table}
SELECT
    t.transaction_id,
    t.customer_id,
    t.card_id,
    t.merchant_id,
    t.amount,
    t.currency,
    t.transaction_timestamp,
    t.channel,
    t.location_country,
    t.transaction_status,
    t.ingestion_timestamp,
    er.rate_to_cad,
    t.amount * er.rate_to_cad AS amount_cad,
    current_timestamp() AS gold_processed_at
FROM {transactions_table} t
LEFT JOIN {exchange_rates_table} er
    ON t.currency = er.currency
   AND CAST(t.transaction_timestamp AS DATE) = er.rate_date
""")

print(f"Gold transactions enriched created: {target_table}")