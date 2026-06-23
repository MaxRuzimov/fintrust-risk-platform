# Databricks notebook source
# 03_transform_gold/06_merchant_risk_summary.py
# Thin wiring: calls src/gold/build_merchant_risk_summary.run_merchant_risk_summary

from src.common.config import load_config, table_name
from src.gold.build_merchant_risk_summary import run_merchant_risk_summary

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
config = load_config(env)

rows = run_merchant_risk_summary(
    spark,
    merchants_table    = table_name(config, "silver", "merchants"),
    transactions_table = table_name(config, "silver", "transactions"),
    fraud_alerts_table = table_name(config, "silver", "fraud_alerts"),
    target_table       = table_name(config, "gold", "merchant_risk_summary"),
    recon_table        = table_name(config, "audit", "reconciliation_results"),
    health_table       = table_name(config, "audit", "pipeline_health"),
)
print(f"merchant_risk_summary rows written: {rows}")