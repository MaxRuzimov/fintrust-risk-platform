# Databricks notebook source

catalog      = "dbw_fintrust_platform_dev"
source_table = f"`{catalog}`.`gold`.`transactions_enriched`"
target_table = f"`{catalog}`.`gold`.`fraud_risk_summary`"

spark.sql(f"""
INSERT OVERWRITE {target_table}
WITH risk_scored AS (
    SELECT
        CAST(transaction_timestamp AS DATE) AS transaction_date,
        amount_cad,
        CASE
            WHEN amount_cad >= 5000 THEN 'HIGH'
            WHEN amount_cad >= 1000 THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level
    FROM {source_table}
)
SELECT
    transaction_date,
    risk_level,
    COUNT(*) AS transaction_count,
    SUM(amount_cad) AS total_amount_cad,
    current_timestamp() AS processed_at
FROM risk_scored
GROUP BY
    transaction_date,
    risk_level
""")

print(f"Fraud risk summary refreshed: {target_table}")