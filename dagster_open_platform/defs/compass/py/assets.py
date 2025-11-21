"""Compass Dagster Plus data export assets."""

import dagster as dg
from dagster_open_platform.utils.environment_helpers import get_database_for_environment
from dagster_snowflake import SnowflakeResource

# List of tables to export from compass_dagster_plus_tables
COMPASS_TABLES = [
    "dagster_plus_runs",
    "dagster_plus_error_logs",
    "dagster_plus_asset_materializations",
    "dagster_plus_steps",
    "dagster_plus_asset_dependencies",
    "dagster_plus_assets",
]

# Determine the database name at module load time (same pattern as dbt translator)
# This will be PURINA, PURINA_DEV, PURINA_CLONE_*, or SANDBOX depending on environment
DBT_DATABASE = get_database_for_environment("PURINA").lower()
DBT_SCHEMA = "public"

# Create asset dependencies on the dbt models
DBT_MODEL_DEPS = [
    dg.AssetKey([DBT_DATABASE, DBT_SCHEMA, table_name]) for table_name in COMPASS_TABLES
]


@dg.asset(
    key=["compass", "dagster_plus_tables_gcs_export"],
    deps=DBT_MODEL_DEPS,
    group_name="compass_exports",
    compute_kind="snowflake",
    description=(
        "Exports Compass Dagster Plus tables to GCS for each organization. "
        "Data is partitioned by organization_id and exported to "
        "@DAGSTER_PLUS_OPERATIONAL_DATA/{org_id}/{table_name}/ in parquet format."
    ),
    automation_condition=dg.AutomationCondition.on_cron("0 2 * * *"),  # Daily at 2 AM
    tags={"dagster/kind/snowflake": "", "dagster/kind/gcs": ""},
    owners=["team:data"],
)
def compass_dagster_plus_tables_gcs_export(
    context: dg.AssetExecutionContext,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """Export Compass Dagster Plus tables to GCS for each organization.

    This asset:
    1. Reads the list of organization IDs from the dagster_plus_organization_ids_compass_context seed
    2. For each organization and each table, executes a COPY INTO command to export data to GCS
    3. Exports data to @DAGSTER_PLUS_OPERATIONAL_DATA/{org_id}/{table_name}/ in parquet format
    """
    database = "COMPASS_DAGSTER_PLUS_TABLES"
    schema = "PUBLIC"
    stage_name = "DAGSTER_PLUS_OPERATIONAL_DATA"

    context.log.info(f"Using database: {database}, schema: {schema}")

    total_rows_exported = 0
    export_count = 0

    with snowflake.get_connection() as conn:
        cursor = conn.cursor()

        # Set the database context
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")

        # Get the list of organization IDs
        org_query = """
        SELECT DISTINCT organization_id
        FROM PURINA.PUBLIC.dagster_plus_organization_ids_compass_context
        ORDER BY organization_id
        """

        context.log.info("Fetching organization IDs")
        cursor.execute(org_query)
        org_ids = [row[0] for row in cursor.fetchall()]

        context.log.info(f"Found {len(org_ids)} organizations to export: {org_ids}")

        # Export each table for each organization
        for org_id in org_ids:
            for table_name in COMPASS_TABLES:
                gcs_path = f"@GOOGLE_CLOUD_STORAGE.COMPASS_PROSPECTOR.DAGSTER_PLUS_OPERATIONAL_DATA/{org_id}/{table_name}/"
                full_table_name = f"{database}.{schema}.{table_name}"

                # Get column information to handle TIMESTAMP_TZ columns
                column_query = f"""
                SELECT column_name, data_type
                FROM {database}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = '{schema}'
                AND table_name = '{table_name.upper()}'
                ORDER BY ordinal_position
                """
                cursor.execute(column_query)
                columns = cursor.fetchall()

                # Build SELECT statement with timestamp conversions
                select_cols = []
                for col_name, col_type in columns:
                    if "TIMESTAMP_TZ" in col_type or "TIMESTAMP_LTZ" in col_type:
                        select_cols.append(f"{col_name}::TIMESTAMP_NTZ AS {col_name}")
                    else:
                        select_cols.append(col_name)

                select_statement = ", ".join(select_cols)

                # Build the COPY INTO command
                copy_sql = f"""
                COPY INTO {gcs_path}
                FROM (
                    SELECT {select_statement}
                    FROM {full_table_name}
                    WHERE organization_id = {org_id}
                )
                FILE_FORMAT = (TYPE = PARQUET)
                OVERWRITE = TRUE
                """

                context.log.info(f"Exporting {table_name} for org {org_id} to {gcs_path}")

                # Execute the COPY INTO command
                cursor.execute(copy_sql)
                result = cursor.fetchone()

                # Parse the result - Snowflake returns: (file_name, file_size, row_count)
                if result and len(result) >= 3:
                    file_size = result[1]
                    row_count = result[2]

                    context.log.info(
                        f"Exported {row_count} rows from {table_name} for org {org_id} "
                        f"({file_size} bytes)"
                    )

                    total_rows_exported += row_count
                    export_count += 1

    context.log.info(
        f"Export complete: {export_count} tables exported, {total_rows_exported} total rows"
    )

    return dg.MaterializeResult(
        metadata={
            "total_organizations": dg.MetadataValue.int(len(org_ids)),
            "total_tables_exported": dg.MetadataValue.int(export_count),
            "total_rows_exported": dg.MetadataValue.int(total_rows_exported),
            "organizations": dg.MetadataValue.text(", ".join(map(str, org_ids))),
            "stage_name": dg.MetadataValue.text(stage_name),
        }
    )
