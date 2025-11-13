import json
import tempfile
import uuid
from pathlib import Path

import dagster as dg
from dagster.components import definitions
from dagster_open_platform.defs.scout.resources.scoutos_resource import ScoutosResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource

scout_queries_daily_partition = dg.DailyPartitionsDefinition(start_date="2025-01-23")


@dg.asset(
    key=["scout", "queries", "run_logs"],
    partitions_def=scout_queries_daily_partition,
    group_name="scoutos",
    description="ScoutOS App Runs",
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),
    kinds={"github", "scout", "snowflake"},
    owners=["team:devrel"],
    tags={"dagster/concurrency_key": "scoutos_app_runs"},
    backfill_policy=dg.BackfillPolicy.single_run(),
)
def scoutos_app_runs(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
    scoutos: ScoutosResource,
) -> dg.MaterializeResult:
    start, end = context.partition_time_window

    database_name = get_database_for_environment("SCOUT")
    schema_name = get_schema_for_environment("QUERIES")
    table_name = "RUN_LOGS"
    stage_name = f"SCOUTOS_STAGE_{uuid.uuid4().hex.upper()}"

    context.log.info(f"Database name: {database_name}")
    context.log.info(f"Schema name: {schema_name}")
    context.log.info(f"Table name: {table_name}")
    context.log.info(f"Stage name: {stage_name}")

    # Define column names and types in one place
    table_schema = {
        "workflow_display_name": "VARCHAR",
        "workflow_run_id": "VARCHAR",
        "workflow_id": "VARCHAR",
        "session_id": "VARCHAR",
        "cost": "FLOAT",
        "blocks": "VARIANT",
        "elapsed_time_ms": "BIGINT",
        "timestamp_start": "TIMESTAMP",
        "timestamp_end": "TIMESTAMP",
    }

    # Extract just the column names for data processing
    required_columns = list(table_schema.keys())

    # Batch processing parameters
    batch_size = 500
    batch_data = []
    rows_loaded = 0
    total_rows = 0
    jsonl_files = []

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Set up Snowflake connection and create stage before processing data
            with snowflake.get_connection() as conn:
                cursor = conn.cursor()

                # Create schema if not exists
                create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}"
                cursor.execute(create_schema_sql)

                # Create internal stage
                create_stage_sql = (
                    f"CREATE OR REPLACE STAGE {database_name}.{schema_name}.{stage_name}"
                )
                cursor.execute(create_stage_sql)
                context.log.info(f"Created stage: {stage_name}")

                # Stream data and process in batches
                for obj in scoutos.get_runs_stream(start, end):
                    # Skip if workflow_id is None
                    if not obj.get("workflow_id"):
                        continue

                    batch_data.append(obj)

                    # Process batch when it reaches batch_size
                    if len(batch_data) >= batch_size:
                        jsonl_file, batch_rows = _process_and_upload_batch(
                            batch_data,
                            temp_dir,
                            len(jsonl_files),
                            cursor,
                            database_name,
                            schema_name,
                            stage_name,
                            context,
                        )
                        jsonl_files.append(jsonl_file)
                        total_rows += batch_rows
                        batch_data = []

                # Process remaining data in the last batch
                if batch_data:
                    jsonl_file, batch_rows = _process_and_upload_batch(
                        batch_data,
                        temp_dir,
                        len(jsonl_files),
                        cursor,
                        database_name,
                        schema_name,
                        stage_name,
                        context,
                        is_final=True,
                    )
                    jsonl_files.append(jsonl_file)
                    total_rows += batch_rows

                context.log.info(f"Total rows processed: {total_rows}")
                context.log.info(f"Total JSONL files created and uploaded: {len(jsonl_files)}")

                if total_rows == 0:
                    context.log.info("No data to process")
                    # Clean up empty stage
                    drop_stage_sql = (
                        f"DROP STAGE IF EXISTS {database_name}.{schema_name}.{stage_name}"
                    )
                    cursor.execute(drop_stage_sql)
                    return dg.MaterializeResult(
                        metadata={
                            "row_count": dg.MetadataValue.int(0),
                            "jsonl_files_created": dg.MetadataValue.int(0),
                        }
                    )

                # Create table if not exists using the schema definition
                column_definitions = [
                    f"{col_name} {col_type}" for col_name, col_type in table_schema.items()
                ]
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
                    {", ".join(column_definitions)}
                )
                """
                cursor.execute(create_table_sql)
                context.log.info(
                    f"Created table {database_name}.{schema_name}.{table_name} if not exists"
                )

                # Delete existing partition data
                delete_query = f"""
                DELETE FROM {database_name}.{schema_name}.{table_name}
                WHERE DATE(timestamp_end) BETWEEN '{start.strftime("%Y-%m-%d")}' AND '{end.strftime("%Y-%m-%d")}'
                """
                cursor.execute(delete_query)
                context.log.info(f"Deleted partition data from {start} to {end}")

                # Copy data from stage to table with proper JSON field mapping
                # Generate column list and SELECT statement from table_schema
                column_list = ", ".join(table_schema.keys())
                select_fields = ",\n                        ".join(
                    [f"$1:{col_name}::{col_type}" for col_name, col_type in table_schema.items()]
                )

                # Fail on error
                copy_sql = f"""
                COPY INTO {database_name}.{schema_name}.{table_name} (
                    {column_list}
                )
                FROM (
                    SELECT 
                        {select_fields}
                    FROM @{database_name}.{schema_name}.{stage_name}
                )
                FILE_FORMAT = (TYPE = 'JSON')
                ON_ERROR = 'ABORT_STATEMENT'
                """
                result = cursor.execute(copy_sql)
                fetch_result = cursor.fetchone()
                rows_loaded = (
                    fetch_result[3] if fetch_result else 0
                )  # Third column contains rows loaded
                context.log.info(f"Loaded {rows_loaded} rows from stage to table")

                # Clean up stage
                drop_stage_sql = f"DROP STAGE IF EXISTS {database_name}.{schema_name}.{stage_name}"
                cursor.execute(drop_stage_sql)
                context.log.info(f"Dropped stage: {stage_name}")

        except Exception as e:
            context.log.error(f"Error processing data: {e}")
            # Clean up stage on error
            try:
                with snowflake.get_connection() as conn:
                    cursor = conn.cursor()
                    drop_stage_sql = (
                        f"DROP STAGE IF EXISTS {database_name}.{schema_name}.{stage_name}"
                    )
                    cursor.execute(drop_stage_sql)
            except:
                pass
            raise e

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(total_rows),
            "jsonl_files_created": dg.MetadataValue.int(len(jsonl_files)),
            "rows_loaded_to_snowflake": dg.MetadataValue.int(
                rows_loaded if "rows_loaded" in locals() else 0
            ),
        }
    )


def _write_batch_to_jsonl(batch_data: list[dict], temp_dir: str, batch_num: int) -> str:
    """Helper function to write a batch of data to a JSONL file."""
    jsonl_file = str(Path(temp_dir) / f"batch_{batch_num:04d}.jsonl")

    with open(jsonl_file, "w") as f:
        for obj in batch_data:
            f.write(json.dumps(obj) + "\n")

    return jsonl_file


def _process_and_upload_batch(
    batch_data: list[dict],
    temp_dir: str,
    batch_num: int,
    cursor,
    database_name: str,
    schema_name: str,
    stage_name: str,
    context,
    is_final: bool = False,
) -> tuple[str, int]:
    """Helper function to process a batch, write to JSONL, and upload to Snowflake stage."""
    jsonl_file = _write_batch_to_jsonl(batch_data, temp_dir, batch_num)

    # Immediately upload JSONL to stage
    put_sql = f"PUT file://{jsonl_file} @{database_name}.{schema_name}.{stage_name}"
    cursor.execute(put_sql)

    batch_type = "final batch" if is_final else "batch"
    context.log.info(f"Processed and uploaded {batch_type} {batch_num + 1}: {len(batch_data)} rows")

    return jsonl_file, len(batch_data)


@dg.asset_check(
    asset=scoutos_app_runs,
    description="Check that workflow_run_id values are unique in the scoutos_app_runs table",
)
def scoutos_app_runs_workflow_run_id_uniqueness_check(
    context: dg.AssetCheckExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.AssetCheckResult:
    """Check that workflow_run_id values are unique in the scoutos_app_runs table."""
    database_name = get_database_for_environment("SCOUT")
    schema_name = get_schema_for_environment("QUERIES")
    table_name = "RUN_LOGS"

    query = f"""
    SELECT workflow_run_id, COUNT(*) as count
    FROM {database_name}.{schema_name}.{table_name}
    WHERE workflow_run_id IS NOT NULL
    GROUP BY workflow_run_id
    HAVING COUNT(*) > 1
    """

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        duplicates = cursor.fetchall()

        if duplicates:
            duplicate_ids = [row[0] for row in duplicates]
            duplicate_counts = {row[0]: row[1] for row in duplicates}

            context.log.warning(f"Found {len(duplicates)} duplicate workflow_run_id values")
            context.log.warning(f"Duplicate workflow_run_ids: {duplicate_ids}")

            return dg.AssetCheckResult(
                passed=False,
                description=f"Found {len(duplicates)} workflow_run_id values with duplicates",
                metadata={
                    "duplicate_count": dg.MetadataValue.int(len(duplicates)),
                    "duplicate_workflow_run_ids": dg.MetadataValue.json(duplicate_counts),
                },
            )
        else:
            context.log.info("All workflow_run_id values are unique")
            return dg.AssetCheckResult(
                passed=True,
                description="All workflow_run_id values are unique",
                metadata={
                    "duplicate_count": dg.MetadataValue.int(0),
                },
            )


@definitions
def defs():
    return dg.Definitions(
        assets=[scoutos_app_runs],
        asset_checks=[scoutos_app_runs_workflow_run_id_uniqueness_check],
    )
