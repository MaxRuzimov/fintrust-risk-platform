import hashlib
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sha2, concat_ws, lit, regexp_replace


def hash_column(df: DataFrame, column_name: str, salt: str = "fintrust") -> DataFrame:
    """
    Hash a sensitive column using SHA-256.
    """
    return df.withColumn(
        f"{column_name}_hash",
        sha2(concat_ws(":", col(column_name).cast("string"), lit(salt)), 256)
    )


def mask_email(df: DataFrame, column_name: str = "email") -> DataFrame:
    """
    Mask email username but keep domain.
    Example: john@gmail.com -> ***@gmail.com
    """
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"^[^@]+", "***")
    )


def mask_phone(df: DataFrame, column_name: str = "phone") -> DataFrame:
    """
    Mask phone digits.
    """
    return df.withColumn(
        column_name,
        regexp_replace(col(column_name), r"\d", "*")
    )


def apply_customer_pii_masking(df: DataFrame) -> DataFrame:
    """
    Apply standard customer PII masking.
    """
    df = hash_column(df, "customer_id")
    df = mask_email(df, "email")
    df = mask_phone(df, "phone")
    return df