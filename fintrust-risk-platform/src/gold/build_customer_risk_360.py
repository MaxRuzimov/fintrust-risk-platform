"""
Gold: customer_risk_360 — one row per customer, a full risk profile.

Joins three silver tables on customer_id:
  customers_scd2 (is_current=true)  -> spine: risk tier, KYC, country
  transactions (agg per customer)   -> behaviour: count, volume, distinct merchants
  fraud_alerts (agg per customer)   -> alerts: count, max severity

Design:
  - SCD2 filtered to is_current=true: a 360 view uses each customer's CURRENT
    state, not their history.
  - customers is the spine with LEFT joins, so a customer with zero transactions
    or zero alerts still appears (with zeros), never vanishes.
  - aggregate transactions/alerts to one-row-per-customer BEFORE joining, to
    avoid row explosion.
  - risk_flag is a native Column expression (factory pattern), not a UDF.
"""
import uuid
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce, col, count, countDistinct, current_timestamp, lit, max as _max,
    sum as _sum, when,
)

from src.common.monitoring import write_pipeline_health
from src.common.reconciliation import write_reconciliation_result

# Map alert severity strings to a numeric rank so we can take a MAX.
_SEVERITY_RANK = (
    when(col("alert_severity") == "CRITICAL", 4)
    .when(col("alert_severity") == "HIGH", 3)
    .when(col("alert_severity") == "MEDIUM", 2)
    .when(col("alert_severity") == "LOW", 1)
    .otherwise(0)
)


def risk_flag(risk_tier_col: Column, alert_count_col: Column) -> Column:
    """
    Derived overall risk flag. A customer is HIGH risk if their KYC risk tier is
    HIGH or they have any fraud alerts; MEDIUM if tier is MEDIUM; else LOW.
    Native Column expression — testable, fast, Catalyst-visible.
    """
    return (
        when((risk_tier_col == "HIGH") | (alert_count_col > 0), "HIGH")
        .when(risk_tier_col == "MEDIUM", "MEDIUM")
        .otherwise("LOW")
    )


def build_customer_risk_360(
    customers_df: DataFrame,
    transactions_df: DataFrame,
    fraud_alerts_df: DataFrame,
) -> DataFrame:
    """Pure transform: three silver DataFrames -> the 360 profile. Testable."""

    # current customers only
    current_customers = customers_df.filter(col("is_current") == True)  # noqa: E712

    # transaction behaviour aggregated per customer
    txn_agg = (
        transactions_df.groupBy("customer_id")
        .agg(
            count("*").alias("transaction_count"),
            _sum("amount").alias("total_amount"),
            countDistinct("merchant_id").alias("distinct_merchants"),
        )
    )

    # fraud alerts aggregated per customer
    alert_agg = (
        fraud_alerts_df.groupBy("customer_id")
        .agg(
            count("*").alias("alert_count"),
            _max(_SEVERITY_RANK).alias("max_alert_severity_rank"),
        )
    )

    profile = (
        current_customers.alias("c")
        .join(txn_agg.alias("t"), "customer_id", "left")
        .join(alert_agg.alias("a"), "customer_id", "left")
        # fill nulls from the LEFT joins with zeros
        .withColumn("transaction_count", coalesce(col("transaction_count"), lit(0)))
        .withColumn("total_amount", coalesce(col("total_amount"), lit(0.0)))
        .withColumn("distinct_merchants", coalesce(col("distinct_merchants"), lit(0)))
        .withColumn("alert_count", coalesce(col("alert_count"), lit(0)))
        .withColumn("max_alert_severity_rank", coalesce(col("max_alert_severity_rank"), lit(0)))
        .withColumn("overall_risk_flag", risk_flag(col("customer_risk_tier"), col("alert_count")))
        .withColumn("processed_at", current_timestamp())
        .select(
            "customer_id", "customer_name", "customer_country", "customer_risk_tier",
            "kyc_status", "transaction_count", "total_amount", "distinct_merchants",
            "alert_count", "max_alert_severity_rank", "overall_risk_flag", "processed_at",
        )
    )
    return profile


def run_customer_risk_360(
    spark: SparkSession,
    customers_table: str,
    transactions_table: str,
    fraud_alerts_table: str,
    target_table: str,
    recon_table: str,
    health_table: str,
) -> int:
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    rows_written = 0
    try:
        customers_df = spark.table(customers_table)
        transactions_df = spark.table(transactions_table)
        fraud_alerts_df = spark.table(fraud_alerts_table)

        profile = build_customer_risk_360(customers_df, transactions_df, fraud_alerts_df)
        profile.write.format("delta").mode("overwrite").saveAsTable(target_table)
        rows_written = spark.table(target_table).count()

        source_count = customers_df.filter(col("is_current") == True).count()  # noqa: E712
        write_reconciliation_result(
            spark, recon_table, "customer_risk_360", "gold_aggregation",
            source_count, rows_written,
        )
        write_pipeline_health(
            spark, health_table, "customer_risk_360", "gold", run_id, "SUCCESS",
            start_time, datetime.now(timezone.utc), records_processed=rows_written,
        )
    except Exception as e:
        write_pipeline_health(
            spark, health_table, "customer_risk_360", "gold", run_id, "FAILED",
            start_time, datetime.now(timezone.utc), error_message=str(e),
        )
        raise
    return rows_written