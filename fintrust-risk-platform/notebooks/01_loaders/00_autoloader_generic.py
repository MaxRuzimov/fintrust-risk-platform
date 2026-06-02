# Databricks notebook source
import uuid
from datetime import datetime, timezone
from pyspark.sql.functions import current_timestamp, input_file_name, expr

from src.common.config import load_config, table_name, volume_path, checkpoint_path
from src.common.ingestion import load_ingestion_metadata, get_contract, build_string_schema
from src.common.quality import split_valid_invalid_records
from src.common.contract_validator import validate_contract
from src.common.reconciliation import write_reconciliation_result
from src.common.monitoring import write_pipeline_health

dbutils.widgets.text("env", "dev")
dbutils.widgets.text("source_name", "")

env         = dbutils.widgets.get("env")
source_name = dbutils.widgets.get("source_name")

config   = load_config(env)
metadata = load_ingestion_metadata()[source_name]

src_cfg       = config["sources"][metadata["source_config_key"]]
contract      = get_contract(metadata["contract"])
required_cols = metadata["required_columns"]
spark_schema  = build_string_schema(metadata["schema_columns"])

schema_key, table_key   = metadata["target_table"].split(".")
q_schema_key, q_table_key = metadata["quarantine_table"].split(".")

target_table     = table_name(config, schema_key, table_key)
quarantine_table = table_name(config, q_schema_key, q_table_key)
recon_table      = table_name(config, "audit", "reconciliation_results")
health_table     = table_name(config, "audit", "pipeline_health")

landing_path = volume_path(config, "landing_base_path", src_cfg["landing_path"].rstrip("/"))
checkpoint   = checkpoint_path(config, src_cfg["checkpoint_name"])

run_id = str(uuid.uuid4())
start_time = datetime.now(timezone.utc)

source_count      = spark.read.format("csv").option("header", "true").schema(spark_schema).load(landing_path).count()
before_target     = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
before_quarantine = spark.sql(f"SELECT COUNT(*) FROM {quarantine_table}").collect()[0][0]

source_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", src_cfg.get("file_format", "csv"))
    .option("cloudFiles.schemaLocation", f"{checkpoint}/schema")
    .option("header", "true")
    .schema(spark_schema)
    .load(landing_path)
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("load_id", expr("uuid()"))
)


def process_batch(batch_df, _):
    validate_contract(batch_df, contract, strict=False)
    valid_df, invalid_df = split_valid_invalid_records(batch_df, required_cols)
    valid_df.write.format("delta").mode("append").saveAsTable(target_table)
    if invalid_df.count() > 0:
        (
            invalid_df
            .withColumn("quarantined_at", current_timestamp())
            .write.format("delta").mode("append").saveAsTable(quarantine_table)
        )


try:
    query = (
        source_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    target_count        = spark.sql(f"SELECT COUNT(*) FROM {target_table}").collect()[0][0]
    quarantine_count    = spark.sql(f"SELECT COUNT(*) FROM {quarantine_table}").collect()[0][0]
    records_processed   = target_count - before_target
    records_quarantined = quarantine_count - before_quarantine

    write_reconciliation_result(spark, recon_table, source_name, "landing_to_bronze", source_count, target_count)
    write_pipeline_health(spark, health_table, source_name, "bronze", run_id, "SUCCESS", start_time, datetime.now(timezone.utc), records_processed, records_quarantined)
except Exception as e:
    write_pipeline_health(spark, health_table, source_name, "bronze", run_id, "FAILED", start_time, datetime.now(timezone.utc), error_message=str(e))
    raise

print(f"Loaded {source_name} from {landing_path} into {target_table}")
print(f"Records processed  : {records_processed}")
print(f"Records quarantined: {records_quarantined}")
