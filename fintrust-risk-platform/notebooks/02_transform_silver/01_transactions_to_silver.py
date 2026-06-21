# Databricks notebook source
from src.common.config import load_config, table_name, checkpoint_path
from src.silver.clean_transactions import split_transactions, deduplicate

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
config = load_config(env)

source_table     = table_name(config, "bronze", "transactions_raw")
silver_table     = table_name(config, "silver", "transactions")
quarantine_table = table_name(config, "quarantine", "invalid_transactions")
checkpoint       = checkpoint_path(config, "silver_transactions")

bronze_df = spark.readStream.table(source_table)

def route_batch(batch_df, _batch_id):
    # ONE split drives both writes — clean and reject can never disagree.
    clean_df, reject_df = split_transactions(batch_df)
 
    (deduplicate(clean_df, streaming=False)
        .write.format("delta").mode("append").saveAsTable(silver_table))
 
    (reject_df
        .write.format("delta").mode("append").saveAsTable(quarantine_table))
 
 
query = (
    bronze_df.writeStream
    .foreachBatch(route_batch)            # foreachBatch: writing two tables per batch
    .option("checkpointLocation", checkpoint)
    .trigger(processingTime="10 seconds")
    .start()
)
 
query.awaitTermination()