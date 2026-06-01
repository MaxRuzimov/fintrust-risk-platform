# Databricks notebook source

catalog      = "dbw_fintrust_platform_dev"
source_table = f"`{catalog}`.`gold`.`transactions_enriched`"
target_table = f"`{catalog}`.`gold`.`daily_transaction_summary`"

spark.sql(f"""
INSERT OVERWRITE {target_table}
SELECT
    CAST(transaction_timestamp AS DATE) AS transaction_date,
    currency,
    COUNT(*) AS transaction_count,
    SUM(amount) AS total_amount,
    SUM(amount_cad) AS total_amount_cad,
    current_timestamp() AS processed_at
FROM {source_table}
GROUP BY
    CAST(transaction_timestamp AS DATE),
    currency
""")

print(f"Daily transaction summary refreshed: {target_table}")