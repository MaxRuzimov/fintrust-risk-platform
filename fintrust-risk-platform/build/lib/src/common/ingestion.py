import yaml
from pyspark.sql.types import StructType, StructField, StringType
from src.common.config import project_root


def load_ingestion_metadata() -> dict:
    path = project_root() / "configs" / "ingestion_metadata.yml"
    with open(path) as f:
        return yaml.safe_load(f)


def get_contract(contract_name: str) -> dict:
    from src.common import contracts
    return getattr(contracts, contract_name)


def build_string_schema(columns: list[str]) -> StructType:
    return StructType([StructField(c, StringType()) for c in columns])


def build_domain_rules(rules: list[dict]) -> list:
    from pyspark.sql.functions import col, lit, when
    spark_rules = []
    for rule in rules:
        column = rule["column"]
        op     = rule["op"]
        error  = rule["error"]
        if op == "gt":
            spark_rules.append(when(col(column) <= rule["value"], lit(error)))
        elif op == "isin":
            spark_rules.append(when(~col(column).isin(rule["values"]), lit(error)))
        elif op == "not_null":
            spark_rules.append(when(col(column).isNull(), lit(error)))
    return spark_rules
