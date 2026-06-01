# Databricks notebook source

catalog      = "dbw_fintrust_platform_dev"
source_table = f"`{catalog}`.`bronze`.`customers_raw`"
target_table = f"`{catalog}`.`silver`.`customers_scd2`"

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW customers_stage AS
SELECT
    customer_id,
    customer_name,
    customer_country,
    customer_risk_tier,
    kyc_status,
    CAST(effective_date AS DATE) AS effective_date
FROM {source_table}
WHERE customer_id IS NOT NULL
""")

spark.sql(f"""
MERGE INTO {target_table} AS target
USING customers_stage AS source
ON target.customer_id = source.customer_id
AND target.is_current = true
WHEN MATCHED AND (
       target.customer_name <> source.customer_name
    OR target.customer_country <> source.customer_country
    OR target.customer_risk_tier <> source.customer_risk_tier
    OR target.kyc_status <> source.kyc_status
)
THEN UPDATE SET
    target.valid_to = source.effective_date,
    target.is_current = false,
    target.updated_at = current_timestamp()

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    customer_country,
    customer_risk_tier,
    kyc_status,
    valid_from,
    valid_to,
    is_current,
    created_at,
    updated_at
)
VALUES (
    source.customer_id,
    source.customer_name,
    source.customer_country,
    source.customer_risk_tier,
    source.kyc_status,
    source.effective_date,
    NULL,
    true,
    current_timestamp(),
    current_timestamp()
)
""")

spark.sql(f"""
INSERT INTO {target_table}
SELECT
    source.customer_id,
    source.customer_name,
    source.customer_country,
    source.customer_risk_tier,
    source.kyc_status,
    source.effective_date AS valid_from,
    NULL AS valid_to,
    true AS is_current,
    current_timestamp() AS created_at,
    current_timestamp() AS updated_at
FROM customers_stage source
LEFT JOIN {target_table} target
    ON source.customer_id = target.customer_id
   AND target.is_current = true
WHERE target.customer_id IS NULL
""")

print(f"Applied SCD2 merge from {source_table} into {target_table}")