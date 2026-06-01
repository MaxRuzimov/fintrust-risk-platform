# Databricks notebook source

from pyspark.sql.functions import (
    col,
    current_timestamp,
    to_timestamp,
    upper,
    trim,
    lit,
    concat_ws,
    when
)

catalog      = "dbw_fintrust_platform_dev"
source_table = f"`{catalog}`.`bronze`.`transactions_raw`"
target_table = f"`{catalog}`.`quarantine`.`invalid_transactions`"
checkpoint   = "/Volumes/dbw_fintrust_platform_dev/audit/checkpoints/quarantine_invalid_transactions"

bronze_df = spark.readStream.table(source_table)

clean_df = (
    bronze_df
    .withColumn("currency", upper(trim(col("currency"))))
    .withColumn("channel", upper(trim(col("channel"))))
    .withColumn("transaction_status", upper(trim(col("transaction_status"))))
    .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
)

invalid_df = (
    clean_df
    .filter(
        col("transaction_id").isNull()
        | col("customer_id").isNull()
        | col("merchant_id").isNull()
        | (col("amount") <= 0)
        | (~col("currency").isin("CAD", "USD", "EUR", "GBP"))
        | col("transaction_timestamp").isNull()
    )
)

quarantine_df = (
    invalid_df
    .withColumn(
        "error_reason",
        concat_ws(
            ", ",
            when(col("transaction_id").isNull(), lit("MISSING_TRANSACTION_ID")),
            when(col("customer_id").isNull(), lit("MISSING_CUSTOMER_ID")),
            when(col("merchant_id").isNull(), lit("MISSING_MERCHANT_ID")),
            when(col("amount") <= 0, lit("INVALID_AMOUNT")),
            when(~col("currency").isin("CAD", "USD", "EUR", "GBP"), lit("INVALID_CURRENCY")),
            when(col("transaction_timestamp").isNull(), lit("INVALID_TRANSACTION_TIMESTAMP"))
        )
    )
    .withColumn("quarantined_at", current_timestamp())
)

query = (
    quarantine_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="10 seconds")
    .toTable(target_table)
)

print(f"Source Table : {source_table}")
print(f"Target Table : {target_table}")
print(f"Checkpoint   : {checkpoint}")