"""
Bronze layer: stream transactions from Azure Event Hub into the raw table.

This holds the logic from
notebooks/01_loaders/01_transactions_eventhub_to_bronze.py.

The hard part of this file vs. the silver one: the real work happens inside a
foreachBatch callback. Spark calls that callback as process_batch(batch_df,
batch_id) and will not pass anything else — so the table names the logic needs
cannot arrive as parameters of process_batch. The factory pattern solves it:
make_batch_processor() takes the config, defines process_batch closed over it,
and returns that function. The notebook hands the returned function to Spark;
a test calls the returned function directly on a fake DataFrame, no stream.
"""
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json, lit, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("card_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("amount", StringType()),
    StructField("currency", StringType()),
    StructField("transaction_timestamp", StringType()),
    StructField("channel", StringType()),
    StructField("location_country", StringType()),
    StructField("transaction_status", StringType()),
])
PARSED_FIELDS = [f.name for f in TRANSACTION_SCHEMA.fields]


def parse_transactions(raw_stream_df: DataFrame) -> DataFrame:
    return (
        raw_stream_df
        .withColumn("raw_payload", col("value").cast("string"))
        .withColumn("event_offset", col("offset"))
        .withColumn("data", from_json(col("raw_payload"), TRANSACTION_SCHEMA))
        .select(*[col(f"data.{name}").alias(name) for name in PARSED_FIELDS],
                "raw_payload", "event_offset")
        .withColumn("ingestion_timestamp", current_timestamp())
    )


def read_eventhub_stream(spark: SparkSession, stream_cfg: dict, connection_string: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", stream_cfg["bootstrap_servers"])
        .option("subscribe", stream_cfg["topic"])
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option(
            "kafka.sasl.jaas.config",
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f'required username="$ConnectionString" password="{connection_string}";',
        )
        .option("startingOffsets", "latest")
        .load()
    )


def run_streaming_ingest(
        spark: SparkSession,
        stream_cfg: dict,
        connection_string: str,
        target_table: str,
        checkpoint: str,
        trigger_interval: str = "10 seconds",
):
    raw_stream_df = read_eventhub_stream(spark, stream_cfg, connection_string)
    parsed_df = parse_transactions(raw_stream_df)
    return (
        parsed_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(processingTime=trigger_interval)
        .toTable(target_table)
    )
