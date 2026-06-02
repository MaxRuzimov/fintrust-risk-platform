# Databricks notebook source
from src.common.config import load_config, table_name
from src.common.reconciliation import write_reconciliation_result

dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")

config = load_config(env)
recon_table = table_name(config, "audit", "reconciliation_results")

# Bronze → Silver: transactions
bronze_transactions = spark.table(table_name(config, "bronze", "transactions_raw")).count()
silver_transactions = spark.table(table_name(config, "silver", "transactions")).count()
write_reconciliation_result(spark, recon_table, "transactions_pipeline", "bronze_to_silver", bronze_transactions, silver_transactions)

# Bronze → Silver: customers (current records only)
bronze_customers = spark.table(table_name(config, "bronze", "customers_raw")).count()
silver_customers = spark.table(table_name(config, "silver", "customers_scd2")).filter("is_current = true").count()
write_reconciliation_result(spark, recon_table, "customers_pipeline", "bronze_to_silver", bronze_customers, silver_customers)

# Bronze → Silver: exchange rates
bronze_exchange_rates = spark.table(table_name(config, "bronze", "exchange_rates_raw")).count()
silver_exchange_rates = spark.table(table_name(config, "silver", "exchange_rates")).count()
write_reconciliation_result(spark, recon_table, "exchange_rates_pipeline", "bronze_to_silver", bronze_exchange_rates, silver_exchange_rates)

# Silver → Gold: transactions enriched
silver_transactions_count = spark.table(table_name(config, "silver", "transactions")).count()
gold_transactions = spark.table(table_name(config, "gold", "transactions_enriched")).count()
write_reconciliation_result(spark, recon_table, "transactions_pipeline", "silver_to_gold", silver_transactions_count, gold_transactions)

print(f"Reconciliation results written to: {recon_table}")
