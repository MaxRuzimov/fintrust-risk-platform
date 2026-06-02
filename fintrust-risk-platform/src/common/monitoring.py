from datetime import datetime, timezone
from pyspark.sql import SparkSession


def write_pipeline_health(
    spark: SparkSession,
    pipeline_health_table: str,
    pipeline_name: str,
    layer: str,
    run_id: str,
    status: str,
    start_time: datetime,
    end_time: datetime,
    records_processed: int = 0,
    records_quarantined: int = 0,
    error_message: str | None = None,
) -> None:
    duration_seconds = (end_time - start_time).total_seconds()

    rows = [(
        pipeline_name,
        layer,
        run_id,
        status,
        start_time,
        end_time,
        duration_seconds,
        records_processed,
        records_quarantined,
        error_message,
        datetime.now(timezone.utc)
    )]

    columns = [
        "pipeline_name",
        "layer",
        "run_id",
        "status",
        "start_time",
        "end_time",
        "duration_seconds",
        "records_processed",
        "records_quarantined",
        "error_message",
        "created_at"
    ]

    spark.createDataFrame(rows, columns).write.format("delta").mode("append").saveAsTable(pipeline_health_table)
