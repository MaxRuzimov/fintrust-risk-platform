# Databricks notebook source

from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

catalog           = "dbw_fintrust_platform_dev"
topic_name        = "fintrust-transactions"
bootstrap_servers = "evhns-fintrust-dev.servicebus.windows.net:9093"
target_table      = f"`{catalog}`.`bronze`.`transactions_raw`"
checkpoint        = "/Volumes/dbw_fintrust_platform_dev/audit/checkpoints/transactions_stream"

# ------------------------------------------------------------------
# Secrets
# ------------------------------------------------------------------

eventhub_connection_string = dbutils.secrets.get(
    scope="fintrust-secrets",
    key="eventhub-connection-string"
)

# ------------------------------------------------------------------
# Transaction Schema
# ------------------------------------------------------------------

transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("transaction_timestamp", StringType()),
    StructField("channel", StringType()),
    StructField("location_country", StringType()),
    StructField("transaction_status", StringType())
])

# ------------------------------------------------------------------
# Read Event Hub Stream
# ------------------------------------------------------------------

raw_stream_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        bootstrap_servers
    )
    .option(
        "subscribe",
        topic_name
    )
    .option(
        "kafka.security.protocol",
        "SASL_SSL"
    )
    .option(
        "kafka.sasl.mechanism",
        "PLAIN"
    )
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{eventhub_connection_string}";'
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .load()
)

# ------------------------------------------------------------------
# Parse JSON Payload
# ------------------------------------------------------------------

parsed_df = (
    raw_stream_df
    .select(
        from_json(
            col("value").cast("string"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")
    .withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )
)

# ------------------------------------------------------------------
# Write Bronze
# ------------------------------------------------------------------

query = (
    parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option(
        "checkpointLocation",
        checkpoint
    )
    .trigger(
        processingTime="10 seconds"
    )
    .toTable(target_table)
)

print(f"Target Table : {target_table}")
print(f"Checkpoint   : {checkpoint}")
print(f"Topic        : {topic_name}")