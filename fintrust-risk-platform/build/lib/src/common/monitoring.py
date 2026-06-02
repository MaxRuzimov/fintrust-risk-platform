from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType, LongType, StringType, StructField, StructType, TimestampType
)

_HEALTH_SCHEMA = StructType([
    StructField("pipeline_name",      StringType(),    True),
    StructField("layer",              StringType(),    True),
    StructField("run_id",             StringType(),    True),
    StructField("status",             StringType(),    True),
    StructField("start_time",         TimestampType(), True),
    StructField("end_time",           TimestampType(), True),
    StructField("duration_seconds",   DoubleType(),    True),
    StructField("records_processed",  LongType(),      True),
    StructField("records_quarantined",LongType(),      True),
    StructField("error_message",      StringType(),    True),
    StructField("created_at",         TimestampType(), True),
])


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

    spark.createDataFrame(rows, _HEALTH_SCHEMA).write.format("delta").mode("append").saveAsTable(pipeline_health_table)
