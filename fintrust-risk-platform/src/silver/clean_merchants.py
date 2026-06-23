"""
Silver layer for merchants — clean + validate the merchant dimension.

Mirrors the clean_transactions pattern: split raw bronze rows into clean vs
rejected in ONE pass, so reject_df keeps original STRING types (matching the
quarantine schema) and clean() is applied only to valid rows.

Merchants is a dimension (not a stream), so:
  - no watermark / streaming dedup; we dedup by keeping the latest row per
    merchant_id (idempotent rebuild via INSERT OVERWRITE in the notebook).
  - is_high_risk_merchant arrives as a STRING from the CSV loader and is cast
    to BOOLEAN here in silver (bronze stays raw/string).
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, trim, upper, when

# (condition_true_when_BAD, error_label). Single source of truth for the split
# and the error text. Required identity columns only — risk_category and
# is_high_risk_merchant may be null without rejecting the row.
REJECT_RULES: list[tuple[Column, str]] = [
    (col("merchant_id").isNull() & col("merchant_name").isNull()
        & col("merchant_country").isNull(),                "PARSE_FAILED"),
    (col("merchant_id").isNull(),                          "MISSING_MERCHANT_ID"),
    (col("merchant_name").isNull(),                        "MISSING_MERCHANT_NAME"),
    (col("merchant_category").isNull(),                    "MISSING_MERCHANT_CATEGORY"),
    (col("merchant_country").isNull(),                     "MISSING_MERCHANT_COUNTRY"),
]


def is_bad() -> Column:
    condition = None
    for rule_condition, _label in REJECT_RULES:
        condition = rule_condition if condition is None else condition | rule_condition
    return condition


def _error_reason() -> Column:
    return concat_ws(", ", *[when(cond, lit(label)) for cond, label in REJECT_RULES])


def split_merchants(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Returns (clean_df, reject_df) from ONE evaluation of the rules.

    clean_df  : valid rows, standardised + typed, stamped silver_processed_at.
    reject_df : rule-violating rows, original strings, tagged error_reason +
                quarantined_at, ready to append to quarantine.
    """
    # drop bronze-only metadata that the silver/quarantine tables don't carry
    raw = df.drop("source_file", "load_id", "raw_payload", "event_offset")
    bad = is_bad()

    clean_df = (
        raw.filter(~bad)
        .withColumn("merchant_category", upper(trim(col("merchant_category"))))
        .withColumn("merchant_country", upper(trim(col("merchant_country"))))
        .withColumn("risk_category", upper(trim(col("risk_category"))))
        # CSV gives "true"/"false" as strings -> real boolean in silver
        .withColumn("is_high_risk_merchant", col("is_high_risk_merchant").cast("boolean"))
        .withColumn("silver_processed_at", current_timestamp())
    )
    reject_df = (
        raw.filter(bad)
        .withColumn("error_reason", _error_reason())
        .withColumn("quarantined_at", current_timestamp())
    )
    return clean_df, reject_df


def deduplicate(df: DataFrame) -> DataFrame:
    """
    A dimension feed may contain repeated merchant_ids across files. Keep one
    row per merchant_id. (No watermark — this is batch, not streaming.)
    """
    return df.dropDuplicates(["merchant_id"])