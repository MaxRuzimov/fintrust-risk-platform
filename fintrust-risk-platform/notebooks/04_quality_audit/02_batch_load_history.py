# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from src.common.config import load_config, table_name

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("load_id", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("source_file", "")
dbutils.widgets.text("target_table_name", "")
dbutils.widgets.text("record_count", "0")
dbutils.widgets.text("status", "SUCCESS")
dbutils.widgets.text("error_message", "")

env = dbutils.widgets.get("env")

config = load_config(env)
audit_table = table_name(config, "audit", "batch_load_history")

audit_df = spark.createDataFrame(
    [(
        dbutils.widgets.get("load_id"),
        dbutils.widgets.get("pipeline_name"),
        dbutils.widgets.get("source_name"),
        dbutils.widgets.get("source_file"),
        dbutils.widgets.get("target_table_name"),
        int(dbutils.widgets.get("record_count")),
        dbutils.widgets.get("status"),
        dbutils.widgets.get("error_message")
    )],
    ["load_id", "pipeline_name", "source_name", "source_file", "target_table", "record_count", "status", "error_message"]
).withColumn("load_timestamp", current_timestamp())

audit_df.write.format("delta").mode("append").saveAsTable(audit_table)

print(f"Audit record inserted into {audit_table}")
