from pyspark.sql import SparkSession, DataFrame


def merge_type1(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: list[str],
) -> None:
    """
    Type 1 SCD: upsert with no history.
    Matching records are overwritten; new keys are inserted.
    """
    source_df.createOrReplaceTempView("_cdc_t1_source")

    key_join   = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
    non_key    = [c for c in source_df.columns if c not in key_columns]
    update_set = ", ".join([f"target.{c} = source.{c}" for c in non_key])
    all_cols   = ", ".join(source_df.columns)
    all_vals   = ", ".join([f"source.{c}" for c in source_df.columns])

    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING _cdc_t1_source AS source
        ON {key_join}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({all_cols}) VALUES ({all_vals})
    """)


def merge_type2(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    key_columns: list[str],
    tracked_columns: list[str],
    effective_date_col: str,
) -> None:
    """
    Type 2 SCD: full history tracking.
    Changed records are expired (valid_to set, is_current=false) and a new
    current version is inserted. Brand-new keys are inserted directly.

    The target table must have columns:
        valid_from, valid_to, is_current, created_at, updated_at
    in addition to all payload columns from source_df.
    """
    source_df.createOrReplaceTempView("_cdc_t2_source")

    key_join         = " AND ".join([f"target.{k} = source.{k}" for k in key_columns])
    change_condition = " OR ".join([f"target.{c} <> source.{c}" for c in tracked_columns])
    payload_cols     = [c for c in source_df.columns if c != effective_date_col]
    scd2_cols        = ", ".join(payload_cols + ["valid_from", "valid_to", "is_current", "created_at", "updated_at"])
    scd2_vals        = ", ".join(
        [f"source.{c}" for c in payload_cols]
        + [f"source.{effective_date_col}", "NULL", "true", "current_timestamp()", "current_timestamp()"]
    )
    src_select       = ", ".join([f"source.{c}" for c in payload_cols])
    first_key        = key_columns[0]

    # Step 1: expire changed current records; insert brand-new keys
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING _cdc_t2_source AS source
        ON {key_join} AND target.is_current = true
        WHEN MATCHED AND ({change_condition}) THEN UPDATE SET
            target.valid_to      = source.{effective_date_col},
            target.is_current    = false,
            target.updated_at    = current_timestamp()
        WHEN NOT MATCHED THEN INSERT ({scd2_cols})
        VALUES ({scd2_vals})
    """)

    # Step 2: insert new current versions for the records just expired above
    spark.sql(f"""
        INSERT INTO {target_table}
        SELECT
            {src_select},
            source.{effective_date_col} AS valid_from,
            NULL                         AS valid_to,
            true                         AS is_current,
            current_timestamp()          AS created_at,
            current_timestamp()          AS updated_at
        FROM _cdc_t2_source source
        LEFT JOIN {target_table} target
            ON {key_join} AND target.is_current = true
        WHERE target.{first_key} IS NULL
    """)
