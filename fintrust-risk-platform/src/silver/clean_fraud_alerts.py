"""
Silver layer for fraud_alerts — clean + standardise the alert feed.

Bronze already validated and quarantined at ingestion (the dedicated loader
runs domain rules), and alert_timestamp is already a real TIMESTAMP in bronze.
So silver's job here is light: a defensive second-pass split, standardise
casing, dedup on alert_id, and present a stable clean contract for gold to join
against (gold reads silver, not bronze).

Same split pattern as clean_merchants / clean_transactions for consistency.
"""
from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, concat_ws, current_timestamp, lit, trim, upper, when

VALID_SEVERITIES = ("LOW", "MEDIUM", "HIGH", "CRITICAL")

# (condition_true_when_BAD, error_label). Defensive second pass — bronze already
# caught most of these, but silver guards its own integrity independently.
REJECT_RULES: list[tuple[Column, str]] = [
    (col("alert_id").isNull() & col("customer_id").isNull()
        & col("alert_timestamp").isNull(),                 "PARSE_FAILED"),
    (col("alert_id").isNull(),                             "MISSING_ALERT_ID"),
    (col("customer_id").isNull(),                          "MISSING_CUSTOMER_ID"),
    (col("merchant_id").isNull(),                          "MISSING_MERCHANT_ID"),
    (col("alert_timestamp").isNull(),                      "INVALID_ALERT_TIMESTAMP"),
    (~upper(trim(col("alert_severity"))).isin(*VALID_SEVERITIES),
                                                           "INVALID_ALERT_SEVERITY"),
]


def is_bad() -> Column:
    condition = None
    for rule_condition, _label in REJECT_RULES:
        condition = rule_condition if condition is None else condition | rule_condition
    return condition


def _error_reason() -> Column:
    return concat_ws(", ", *[when(cond, lit(label)) for cond, label in REJECT_RULES])


def split_fraud_alerts(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Returns (clean_df, reject_df) from ONE evaluation of the rules.

    clean_df  : valid rows, standardised casing, stamped silver_processed_at.
    reject_df : rule-violating rows, original values, tagged error_reason +
                quarantined_at.
    """
    raw = df.drop("source_file", "raw_payload", "event_offset")
    bad = is_bad()

    clean_df = (
        raw.filter(~bad)
        .withColumn("alert_type", upper(trim(col("alert_type"))))
        .withColumn("alert_severity", upper(trim(col("alert_severity"))))
        .withColumn("silver_processed_at", current_timestamp())
    )
    reject_df = (
        raw.filter(bad)
        .withColumn("error_reason", _error_reason())
        .withColumn("quarantined_at", current_timestamp())
    )
    return clean_df, reject_df


def deduplicate(df: DataFrame) -> DataFrame:
    """Keep one row per alert_id. Batch feed — no watermark."""
    return df.dropDuplicates(["alert_id"])