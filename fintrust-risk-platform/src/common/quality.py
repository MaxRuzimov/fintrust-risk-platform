from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate_required_columns(df: DataFrame, required_columns: list[str]) -> None:
    """
    Validate that all required columns exist in the DataFrame.
    """
    missing_columns = [c for c in required_columns if c not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")


def filter_not_null(df: DataFrame, required_columns: list[str]) -> DataFrame:
    """
    Keep only records where required columns are not null.
    """
    condition = None

    for column_name in required_columns:
        current_condition = col(column_name).isNotNull()
        condition = current_condition if condition is None else condition & current_condition

    return df.filter(condition)


def filter_duplicates(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """
    Remove duplicate records based on business key columns.
    """
    return df.dropDuplicates(key_columns)


def split_valid_invalid_records(
    df: DataFrame,
    required_columns: list[str]
) -> tuple[DataFrame, DataFrame]:
    """
    Split records into valid and invalid DataFrames.
    Invalid = at least one required column is null.
    """
    valid_condition = None

    for column_name in required_columns:
        current_condition = col(column_name).isNotNull()
        valid_condition = current_condition if valid_condition is None else valid_condition & current_condition

    valid_df = df.filter(valid_condition)
    invalid_df = df.filter(~valid_condition)

    return valid_df, invalid_df