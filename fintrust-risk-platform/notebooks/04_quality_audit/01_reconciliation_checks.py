# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
bronze_customers     = table_name(config, "bronze", "customers_raw")
silver_customers     = table_name(config, "silver", "customers_scd2")
reconciliation_table = table_name(config, "audit", "reconciliation_results")

source_count = spark.table(bronze_customers).count()

target_count = (
    spark.table(silver_customers)
    .filter("is_current = true")
    .count()
)

difference = source_count - target_count
status = "PASS" if difference == 0 else "FAIL"

recon_df = spark.createDataFrame(
    [(
        "customers_recon",
        "customers_bronze_to_silver",
        "customers_file",
        source_count,
        target_count,
        difference,
        status
    )],
    ["check_id", "pipeline_name", "source_name", "source_count", "target_count", "difference", "status"]
).withColumn("check_timestamp", current_timestamp())

recon_df.write.format("delta").mode("append").saveAsTable(reconciliation_table)

print(f"Reconciliation status: {status}")
print(f"Source count: {source_count}")
print(f"Target count: {target_count}")
print(f"Difference: {difference}")
