import os
import yaml
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_config(env: str = "dev") -> dict:
    # FINTRUST_CONFIG_PATH is set on Databricks clusters so the installed
    # wheel can find configs deployed to the workspace file path.
    config_root = os.environ.get("FINTRUST_CONFIG_PATH")
    if config_root:
        config_path = Path(config_root) / f"{env}.yml"
    else:
        config_path = project_root() / "configs" / f"{env}.yml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def table_name(config: dict, schema_key: str, table: str) -> str:
    catalog = config["catalog"]
    schema = config["schemas"][schema_key]
    return f"`{catalog}`.`{schema}`.`{table}`"


def volume_path(config: dict, path_key: str, sub_path: str = "") -> str:
    base_path = config["storage"][path_key].rstrip("/")
    if sub_path:
        return f"{base_path}/{sub_path.strip('/')}/"
    return base_path


def checkpoint_path(config: dict, checkpoint_name: str) -> str:
    base_path = config["storage"]["checkpoint_base_path"].rstrip("/")
    return f"{base_path}/{checkpoint_name}"