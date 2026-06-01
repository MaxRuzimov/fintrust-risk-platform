# Databricks notebook source

catalog = "dbw_fintrust_platform_dev"
reference_schema = "reference"
audit_schema = "audit"

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{reference_schema}`.`landing`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{reference_schema}`.`archive`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{audit_schema}`.`checkpoints`")

print(f"Volumes created in catalog: {catalog}")
