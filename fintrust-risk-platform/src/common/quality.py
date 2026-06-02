from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, concat_ws, array, array_remove, size


def validate_required_columns(df: DataFrame, required_columns: list[str]) -> None:
    missing_columns = [c for c in required_columns if c not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def filter_not_null(df: DataFrame, required_columns: list[str]) -> DataFrame:
    condition = None
    for column_name in required_columns:
        current_condition = col(column_name).isNotNull()
        condition = current_condition if condition is None else condition & current_condition
    return df.filter(condition)


def filter_duplicates(df: DataFrame, key_columns: list[str]) -> DataFrame:
    return df.dropDuplicates(key_columns)


def split_valid_invalid_records(
    df: DataFrame,
    required_columns: list[str]
) -> tuple[DataFrame, DataFrame]:
    valid_condition = None
    for column_name in required_columns:
        current_condition = col(column_name).isNotNull()
        valid_condition = current_condition if valid_condition is None else valid_condition & current_condition

    valid_df = df.filter(valid_condition)

    error_reasons = [
        when(col(c).isNull(), lit(f"MISSING_{c.upper()}"))
        for c in required_columns
    ]

    invalid_df = (
        df.filter(~valid_condition)
        .withColumn("error_reason", concat_ws(", ", *error_reasons))
    )

    return valid_df, invalid_df


def add_domain_validation_errors(df: DataFrame, validation_rules: list) -> DataFrame:
    return df.withColumn(
        "quality_errors",
        array_remove(array(*validation_rules), None)
    )


def split_domain_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid_df = (
        df.filter(size(col("quality_errors")) == 0)
        .drop("quality_errors")
    )
    invalid_df = (
        df.filter(size(col("quality_errors")) > 0)
        .withColumn("error_reason", concat_ws(", ", col("quality_errors")))
        .drop("quality_errors")
    )
    return valid_df, invalid_df
