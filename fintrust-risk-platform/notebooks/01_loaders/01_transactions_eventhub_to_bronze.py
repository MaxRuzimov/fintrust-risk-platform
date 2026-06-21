# Databricks notebook source
from src.common.config import load_config, table_name, checkpoint_path
from src.bronze.ingest_transactions_stream import run_streaming_ingest

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
config = load_config(env)
stream_cfg = config["sources"]["transactions_stream"]

connection_string = dbutils.secrets.get(
    scope=stream_cfg["connection_string_secret_scope"],
    key=stream_cfg["connection_string_secret_key"],
)

query = run_streaming_ingest(
    spark,
    stream_cfg=stream_cfg,
    connection_string=connection_string,
    target_table=table_name(config, "bronze", "transactions_raw"),
    checkpoint=checkpoint_path(config, stream_cfg["checkpoint_name"]),
)

query.awaitTermination()
