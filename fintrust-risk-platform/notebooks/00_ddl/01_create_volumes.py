# Databricks notebook source
from src.common.config import load_config

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
catalog          = config["catalog"]
reference_schema = config["schemas"]["reference"]
audit_schema     = config["schemas"]["audit"]

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{reference_schema}`.`landing`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{reference_schema}`.`archive`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{audit_schema}`.`checkpoints`")

print(f"Volumes created in catalog: {catalog}")
