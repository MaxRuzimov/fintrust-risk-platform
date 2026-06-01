# Databricks notebook source

from pyspark.sql.functions import current_timestamp
dbutils.widgets.text("load_id", "")
dbutils.widgets.text("pipeline_name", "")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("source_file", "")
dbutils.widgets.text("target_table_name", "")
dbutils.widgets.text("record_count", "0")
dbutils.widgets.text("status", "SUCCESS")
dbutils.widgets.text("error_message", "")

catalog     = "dbw_fintrust_platform_dev"
audit_table = f"`{catalog}`.`audit`.`batch_load_history`"

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
    [
        "load_id",
        "pipeline_name",
        "source_name",
        "source_file",
        "target_table",
        "record_count",
        "status",
        "error_message"
    ]
).withColumn("load_timestamp", current_timestamp())

audit_df.write.format("delta").mode("append").saveAsTable(audit_table)

print(f"Audit record inserted into {audit_table}")