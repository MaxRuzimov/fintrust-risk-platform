from pyspark.sql import DataFrame

_TYPE_ALIASES = {
    "int": "integer",
    "long": "bigint",
    "float": "float",
}


def validate_contract(df: DataFrame, contract: dict) -> None:
    df_types = dict(df.dtypes)
    contract_cols = set(contract)
    actual_cols = set(df_types)

    missing_columns = sorted(contract_cols - actual_cols)
    unexpected_columns = sorted(actual_cols - contract_cols)
    wrong_types = [
        f"  {col}: expected '{contract[col]}', got '{df_types[col]}'"
        for col in sorted(contract_cols & actual_cols)
        if _TYPE_ALIASES.get(contract[col], contract[col]) != df_types[col]
    ]

    issues = []
    if missing_columns:
        issues.append(f"Missing columns: {missing_columns}")
    if unexpected_columns:
        issues.append(f"Unexpected columns: {unexpected_columns}")
    if wrong_types:
        issues.append("Wrong data types:\n" + "\n".join(wrong_types))

    if issues:
        raise ValueError("Contract validation failed:\n" + "\n".join(issues))
