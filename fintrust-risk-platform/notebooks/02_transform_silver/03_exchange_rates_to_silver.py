from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)

source_table = table_name(config, "bronze", "exchange_rates_raw")
target_table = table_name(config, "silver", "exchange_rates")

spark.sql(f"""
INSERT OVERWRITE {target_table}
SELECT
    UPPER(TRIM(currency)) AS currency,
    CAST(rate_date AS DATE) AS rate_date,
    CAST(rate_to_cad AS DOUBLE) AS rate_to_cad,
    current_timestamp() AS loaded_at
FROM {source_table}
WHERE currency IS NOT NULL
  AND rate_date IS NOT NULL
  AND rate_to_cad IS NOT NULL
""")

print(f"Transformed exchange rates from {source_table} into {target_table}")