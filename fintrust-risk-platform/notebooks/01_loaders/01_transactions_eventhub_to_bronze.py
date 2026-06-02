# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import col, current_timestamp, from_json, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from src.common.config import load_config, table_name, checkpoint_path
from src.common.quality import (
    split_valid_invalid_records,
    add_domain_validation_errors,
    split_domain_valid_invalid
)
from src.common.monitoring import write_pipeline_health
from src.common.contracts import TRANSACTIONS_CONTRACT
from src.common.contract_validator import validate_contract

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
stream_cfg = config["sources"]["transactions_stream"]

topic_name        = stream_cfg["topic"]
bootstrap_servers = stream_cfg["bootstrap_servers"]
target_table      = table_name(config, "bronze", "transactions_raw")
quarantine_table  = table_name(config, "quarantine", "invalid_transactions")
health_table      = table_name(config, "audit", "pipeline_health")
checkpoint        = checkpoint_path(config, stream_cfg["checkpoint_name"])

eventhub_connection_string = dbutils.secrets.get(
    scope=stream_cfg["connection_string_secret_scope"],
    key=stream_cfg["connection_string_secret_key"]
)

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

required_columns = [
    "transaction_id", "customer_id", "card_id", "merchant_id",
    "amount", "currency", "transaction_timestamp", "channel", "transaction_status"
]

domain_rules = [
    when(col("amount") <= 0, lit("INVALID_AMOUNT")),
    when(~col("currency").isin("CAD", "USD", "EUR", "GBP"), lit("INVALID_CURRENCY")),
]

raw_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic_name)
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{eventhub_connection_string}";'
    )
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = (
    raw_stream_df
    .select(from_json(col("value").cast("string"), transaction_schema).alias("data"))
    .select("data.*")
    .withColumn("ingestion_timestamp", current_timestamp())
)


def process_batch(batch_df, _):
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    records_processed = 0
    records_quarantined = 0
    try:
        validate_contract(batch_df, TRANSACTIONS_CONTRACT, strict=False)

        valid_df, invalid_df = split_valid_invalid_records(batch_df, required_columns)
        valid_df = add_domain_validation_errors(valid_df, domain_rules)
        domain_valid_df, domain_invalid_df = split_domain_valid_invalid(valid_df)

        records_processed = domain_valid_df.count()
        domain_valid_df.write.format("delta").mode("append").saveAsTable(target_table)

        all_invalid_df = invalid_df.unionByName(domain_invalid_df, allowMissingColumns=True)
        records_quarantined = all_invalid_df.count()
        if records_quarantined > 0:
            (
                all_invalid_df
                .withColumn("quarantined_at", current_timestamp())
                .write.format("delta").mode("append").saveAsTable(quarantine_table)
            )

        write_pipeline_health(spark, health_table, "transactions_eventhub", "bronze", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed, records_quarantined)
    except Exception as e:
        write_pipeline_health(spark, health_table, "transactions_eventhub", "bronze", run_id, "FAILED", start_time, datetime.now(timezone.utc), records_processed, records_quarantined, str(e))
        raise


query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Target Table    : {target_table}")
print(f"Quarantine Table: {quarantine_table}")
print(f"Checkpoint      : {checkpoint}")
print(f"Topic           : {topic_name}")
