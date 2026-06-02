from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def write_reconciliation_result(
    spark: SparkSession,
    recon_table: str,
    pipeline_name: str,
    layer: str,
    source_count: int,
    target_count: int
) -> None:
    difference = source_count - target_count
    status = "PASSED" if difference == 0 else "FAILED"

    (
        spark.createDataFrame(
            [(pipeline_name, layer, source_count, target_count, difference, status)],
            ["pipeline_name", "layer", "source_count", "target_count", "difference", "reconciliation_status"]
        )
        .withColumn("validation_timestamp", current_timestamp())
        .write.format("delta").mode("append").saveAsTable(recon_table)
    )
