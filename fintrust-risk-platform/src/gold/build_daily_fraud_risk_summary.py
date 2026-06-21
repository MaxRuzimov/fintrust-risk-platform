import uuid
from datetime import datetime, timezone
 
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.functions import col, count, current_timestamp, sum as _sum, when
 
from src.common.monitoring import write_pipeline_health
from src.common.reconciliation import write_reconciliation_result

HIGH_THRESHOLD_CAD = 5000
MEDIUM_THRESHOLD_CAD = 1000

def risk_label(amount_col: Column) -> Column:
    return (
        when(amount_col >= HIGH_THRESHOLD_CAD, "HIGH")
        .when(amount_col >= MEDIUM_THRESHOLD_CAD, "MEDIUM")
        .otherwise("LOW")
    )

def build_summary(enriched_df: DataFrame) ->DataFrame:
    return (
        enriched_df
        .withColumn("transaction_date", col("transaction_timestamp").cast("date"))
        .withColumn("risk_level", risk_label(col("amount_cad")))
        .groupBy("transaction_date", "risk_level")
        .agg(
            count("*").alias("transaction_count"),
            _sum("amount_cad").alias("total_amount_cad"),
        )
        .withColumn("processed_at", current_timestamp())
    )

def run_froud_risk_summary(
        spark: SparkSession,
        source_table: str,
        target_table: str,
        recon_table: str,
        health_table: str,
)->int:
    run_id = str(uuid.uuid4())
    start_time = datetime.now(timezone.utc)
    rows_written = 0
    try:
        enriched_df = spark.table(source_table)
        source_count = enriched_df.count()
 
        summary_df = build_summary(enriched_df)
        summary_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
        rows_written = spark.table(target_table).count()
 
        write_reconciliation_result(
            spark, recon_table, "fraud_risk_summary", "gold_aggregation",
            source_count, rows_written,
        )
        write_pipeline_health(
            spark, health_table, "fraud_risk_summary", "gold", run_id, "SUCCESS",
            start_time, datetime.now(timezone.utc), records_processed=rows_written,
        )
    except Exception as e:
        write_pipeline_health(
            spark, health_table, "fraud_risk_summary", "gold", run_id, "FAILED",
            start_time, datetime.now(timezone.utc), error_message=str(e),
        )
        raise
    return rows_written