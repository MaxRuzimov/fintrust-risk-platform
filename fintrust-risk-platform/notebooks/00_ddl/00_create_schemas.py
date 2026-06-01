# Databricks notebook source

catalog = "dbw_fintrust_platform_dev"

schemas = ["bronze", "silver", "gold", "audit", "quarantine", "reference", "ml_features"]

for schema in schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    print(f"Created schema: {catalog}.{schema}")
