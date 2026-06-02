# Databricks notebook source
from src.common.config import load_config

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
catalog = config["catalog"]
schemas = list(config["schemas"].values())

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"Created schema: {catalog}.{schema}")
