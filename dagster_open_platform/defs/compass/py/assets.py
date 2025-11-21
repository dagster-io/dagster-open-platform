"""Compass Dagster Plus data export assets."""

import dagster as dg
from dagster_dbt import get_asset_key_for_model
from dagster_gcp import BigQueryResource
from dagster_open_platform.defs.dbt.assets import get_dbt_non_partitioned_models
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

# Get asset dependencies from dbt models using get_asset_key_for_model
DBT_MODEL_DEPS = [
    get_asset_key_for_model([get_dbt_non_partitioned_models()], table_name)
    for table_name in COMPASS_TABLES
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

                # Remove old files from the stage path first
                remove_sql = f"REMOVE {gcs_path}"
                context.log.info(f"Removing old files from {gcs_path}")
                try:
                    cursor.execute(remove_sql)
                    context.log.info(f"Cleared stage path {gcs_path}")
                except Exception as e:
                    context.log.info(f"No files to remove or error clearing stage: {e}")

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
                HEADER = TRUE
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


@dg.asset(
    key=["compass", "dagster_plus_tables_bigquery_load"],
    deps=[compass_dagster_plus_tables_gcs_export],
    group_name="compass_exports",
    compute_kind="bigquery",
    description=(
        "Loads Compass Dagster Plus tables from GCS into BigQuery. "
        "Creates a dataset for each organization (dagster_operational_data_org_<org_id>) "
        "and loads all tables from the corresponding GCS paths."
    ),
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),  # Daily at 3 AM
    tags={"dagster/kind/bigquery": "", "dagster/kind/gcs": ""},
    owners=["team:data"],
)
def compass_dagster_plus_tables_bigquery_load(
    context: dg.AssetExecutionContext,
    bigquery_compass_prospector: BigQueryResource,
    snowflake: SnowflakeResource,
) -> dg.MaterializeResult:
    """Load Compass Dagster Plus tables from GCS into BigQuery for each organization.

    This asset:
    1. Reads the list of organization IDs from the seed table
    2. For each organization, creates a BigQuery dataset named dagster_operational_data_org_<org_id>
    3. Loads each table from GCS Parquet files into BigQuery tables
    """
    database = "COMPASS_DAGSTER_PLUS_TABLES"
    schema = "PUBLIC"
    gcs_bucket = "dagster-plus-operational-data"

    total_rows_loaded = 0
    datasets_created = 0
    tables_loaded = 0

    with bigquery_compass_prospector.get_client() as bq_client:
        # Get BigQuery project ID
        project_id = bq_client.project

        context.log.info(f"Using BigQuery project: {project_id}")

        # Get organization IDs from Snowflake
        with snowflake.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {database}")
            cursor.execute(f"USE SCHEMA {schema}")

            org_query = """
            SELECT DISTINCT organization_id
            FROM PURINA.PUBLIC.dagster_plus_organization_ids_compass_context
            ORDER BY organization_id
            """

            context.log.info("Fetching organization IDs")
            cursor.execute(org_query)
            org_ids = [row[0] for row in cursor.fetchall()]

        context.log.info(f"Found {len(org_ids)} organizations to load: {org_ids}")

        # Process each organization
        for org_id in org_ids:
            dataset_id = f"dagster_operational_data_org_{org_id}"
            dataset_ref = f"{project_id}.{dataset_id}"

            # Create dataset if it doesn't exist
            from google.cloud.bigquery import Dataset

            dataset = Dataset(dataset_ref)
            dataset.location = "US"

            dataset = bq_client.create_dataset(dataset, exists_ok=True)
            context.log.info(f"Created/verified dataset: {dataset_id}")
            datasets_created += 1

            # Load each table
            for table_name in COMPASS_TABLES:
                table_id = f"{dataset_ref}.{table_name}"
                gcs_uri = f"gs://{gcs_bucket}/{org_id}/{table_name}/*.parquet"

                context.log.info(f"Loading {table_name} for org {org_id} from {gcs_uri}")

                # Drop existing table if it exists to ensure schema changes are picked up
                try:
                    bq_client.delete_table(table_id, not_found_ok=True)
                    context.log.info(f"Dropped existing table {table_id}")
                except Exception as e:
                    context.log.warning(f"Could not drop table {table_id}: {e}")

                # Configure the load job
                from google.cloud.bigquery import LoadJobConfig, SourceFormat

                job_config = LoadJobConfig(
                    source_format=SourceFormat.PARQUET,
                    write_disposition="WRITE_EMPTY",  # Only write if table is empty (newly created)
                )

                # Start the load job
                load_job = bq_client.load_table_from_uri(
                    gcs_uri,
                    table_id,
                    job_config=job_config,
                )

                # Wait for the job to complete
                load_job.result()

                # Get the loaded table to check row count
                table = bq_client.get_table(table_id)
                row_count = table.num_rows or 0

                context.log.info(f"Loaded {row_count} rows into {dataset_id}.{table_name}")

                total_rows_loaded += row_count
                tables_loaded += 1

    context.log.info(
        f"Load complete: {datasets_created} datasets, {tables_loaded} tables, "
        f"{total_rows_loaded} total rows"
    )

    return dg.MaterializeResult(
        metadata={
            "total_organizations": dg.MetadataValue.int(len(org_ids)),
            "datasets_created": dg.MetadataValue.int(datasets_created),
            "tables_loaded": dg.MetadataValue.int(tables_loaded),
            "total_rows_loaded": dg.MetadataValue.int(total_rows_loaded),
            "organizations": dg.MetadataValue.text(", ".join(map(str, org_ids))),
            "bigquery_project": dg.MetadataValue.text(project_id),
        }
    )
