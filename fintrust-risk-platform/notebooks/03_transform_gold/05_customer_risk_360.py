# Databricks notebook source
# 03_transform_gold/05_customer_risk_360.py
# Thin wiring: calls src/gold/build_customer_risk_360.run_customer_risk_360

from src.common.config import load_config, table_name
from src.gold.build_customer_risk_360 import run_customer_risk_360

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
config = load_config(env)

rows = run_customer_risk_360(
    spark,
    customers_table    = table_name(config, "silver", "customers_scd2"),
    transactions_table = table_name(config, "silver", "transactions"),
    fraud_alerts_table = table_name(config, "silver", "fraud_alerts"),
    target_table       = table_name(config, "gold", "customer_risk_360"),
    recon_table        = table_name(config, "audit", "reconciliation_results"),
    health_table       = table_name(config, "audit", "pipeline_health"),
)
print(f"customer_risk_360 rows written: {rows}")