"""
Silver layer for transactions — the single validation point in the platform.
 
Bronze now lands everything raw (10 parsed cols + raw_payload + offset). This
module is the ONE place transactions are validated. It reads bronze, splits
rows into clean vs rejected in a single pass, and the notebook writes clean to
silver and rejects to quarantine. Rules live here and nowhere else.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import (
    col, concat_ws, current_timestamp, lit, to_timestamp, trim, upper, when,
)

REJECT_RULES: list[tuple[Column, str]] = [
    (col("transaction_id").isNull() & col("customer_id").isNull()
        & col("amount").isNull(),                          "PARSE_FAILED"),
    (col("transaction_id").isNull(),                       "MISSING_TRANSACTION_ID"),
    (col("customer_id").isNull(),                          "MISSING_CUSTOMER_ID"),
    (col("merchant_id").isNull(),                          "MISSING_MERCHANT_ID"),
    (col("amount").isNull() | (col("amount") <= 0),        "INVALID_AMOUNT"),
    (~col("currency").isin("CAD", "USD", "EUR", "GBP"),    "INVALID_CURRENCY"),
    (col("transaction_timestamp").isNull(),                "INVALID_TIMESTAMP"),
]


def is_bad() -> Column:
    condition = None
    for rule_condition, _lebel in REJECT_RULES:
        condition = rule_condition if condition is None else condition | rule_condition
    return condition

def _error_reason() -> Column:
    return concat_ws(", ", *[when(cond, lit(label)) for cond, label in REJECT_RULES])

def split_transactions(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    The funnel. Returns (clean_df, reject_df) from ONE evaluation of the rules.

    Split happens on raw bronze data so reject_df keeps original STRING types
    (matching the quarantine table schema). clean() is applied only to valid rows.
    clean_df  : rows violating no rule, fully cleaned, stamped with silver_processed_at.
    reject_df : rows violating >= 1 rule, tagged with error_reason and quarantined_at.
    """
    raw = df.drop("raw_payload", "event_offset")
    bad = is_bad()

    clean_df = (
        raw.filter(~bad)
        .withColumn("currency", upper(trim(col("currency"))))
        .withColumn("channel", upper(trim(col("channel"))))
        .withColumn("transaction_status", upper(trim(col("transaction_status"))))
        .withColumn("transaction_timestamp", to_timestamp(col("transaction_timestamp")))
        .withColumn("silver_processed_at", current_timestamp())
    )
    reject_df = (
        raw.filter(bad)
        .withColumn("error_reason", _error_reason())
        .withColumn("quarantined_at", current_timestamp())
    )
    return clean_df, reject_df

def deduplicate(df: DataFrame, streaming: bool = True) -> DataFrame:
    """
    Remove duplicate transaction_ids. streaming=False skips the watermark so a
    static test DataFrame doesn't raise. Applied to the CLEAN set only.
    """
    if streaming:
        df = df.withWatermark("transaction_timestamp", "30 minutes")
    return df.dropDuplicates(["transaction_id"])
