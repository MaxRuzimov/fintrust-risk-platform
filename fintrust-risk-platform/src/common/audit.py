from datetime import datetime, timezone
from pyspark.sql import SparkSession


def write_audit_log(
    spark: SparkSession,
    catalog: str,
    status: str,
    pipeline_name: str,
    source_name: str,
    target_table: str,
    row_count: int = 0,
    error_message: str | None = None,
) -> None:
    """
    Write pipeline execution metadata into audit.pipeline_run_log.
    """

    audit_table = f"{catalog}.audit.batch_load_history"

    audit_data = [{
        "pipeline_name": pipeline_name,
        "source_name": source_name,
        "target_table": target_table,
        "status": status,
        "row_count": row_count,
        "error_message": error_message,
        "logged_at": datetime.now(timezone.utc).isoformat()
    }]

    audit_df = spark.createDataFrame(audit_data)

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(audit_table)