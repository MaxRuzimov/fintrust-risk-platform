# Databricks notebook source
from pyspark.sql.functions import expr
from src.common.config import load_config, table_name, checkpoint_path

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)

transactions_table = table_name(config, "silver", "transactions")
fraud_alerts_table = table_name(config, "bronze", "fraud_alerts_raw")
target_table       = table_name(config, "gold", "transactions_with_fraud_alerts")
checkpoint         = checkpoint_path(config, "transactions_with_fraud_alerts")

transactions = (
    spark.readStream
    .table(transactions_table)
    .withWatermark("transaction_timestamp", "30 minutes")
)

fraud_alerts = (
    spark.readStream
    .table(fraud_alerts_table)
    .withWatermark("alert_timestamp", "30 minutes")
)

joined = (
    transactions.alias("t")
    .join(
        fraud_alerts.alias("f"),
        expr("""
            t.customer_id = f.customer_id
            AND f.alert_timestamp >= t.transaction_timestamp
            AND f.alert_timestamp <= t.transaction_timestamp + INTERVAL 30 MINUTES
        """),
        "leftOuter"
    )
    .selectExpr(
        "t.transaction_id",
        "t.customer_id",
        "t.merchant_id",
        "t.amount",
        "t.currency",
        "t.transaction_timestamp",
        "f.alert_id",
        "f.alert_type",
        "f.alert_severity",
        "f.alert_timestamp",
        "current_timestamp() as processed_at"
    )
)

query = (
    joined.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .toTable(target_table)
)

print(f"Source (transactions) : {transactions_table}")
print(f"Source (fraud alerts) : {fraud_alerts_table}")
print(f"Target                : {target_table}")
print(f"Checkpoint            : {checkpoint}")

query.awaitTermination()
