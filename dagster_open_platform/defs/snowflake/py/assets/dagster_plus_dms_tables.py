"""COPY INTO assets for the Dagster Plus DMS replication tables.

AWS DMS replicates selected ``public.*`` tables from the cloud-prod Postgres
shards to S3 (``s3://dagster-dms-cloud-prod``), exposed in Snowflake through the
``aws.<schema>.dagster_plus_dms_stage`` stage created by the ``dagster_plus_dms``
component (``defs/snowflake/components/dagster_plus_dms``). DMS lands each table
under the ``public/<table>/`` prefix of that stage.

For every replicated table this module defines one asset that loads the parquet
files for that table into a final ``aws.<schema>.dagster_plus__<table>`` table via
``COPY INTO``. Snowflake tracks loaded files natively (``FORCE = FALSE``), so each
file is ingested exactly once across re-runs.

The schema mirrors the stage (see ``_dms_schema_from_env``): ``cloud_prod`` in
prod and ``dev`` everywhere else. In production these assets therefore resolve to
keys / tables like ``aws.cloud_prod.dagster_plus__users``.
"""

import dagster as dg
from dagster.components import definitions
from dagster_open_platform.lib.snowflake.component import _dms_schema_from_env
from dagster_snowflake import SnowflakeResource

# The DMS stage component hardcodes these (component.yaml), so we match them here
# to keep asset keys and SQL aligned with the stage asset.
_AWS_DB = "aws"
_ROLE = "AWS_WRITER"
_STAGE_NAME = "dagster_plus_dms_stage"
# DMS writes files under a prefix named after the source schema ("public").
_STAGE_PREFIX = "public"
_TABLE_PREFIX = "dagster_plus__"

# Tables replicated into the `public/` prefix of the DMS stage. Keep in sync with
# `main_public_tables` plus the xregion `organizations` task in
# infra/terragrunt/cloud-prod/us-west-2/cloud-prod/dms/terragrunt.hcl.
_DMS_TABLES = [
    "alerts_alert_policies_new",
    "customer_info",
    "deployments",
    "enterprise_user_managed_expansions",
    "onboarding_checklist",
    "organizations",
    "serverless_agents",
    "teams",
    "teams_users",
    "users",
    "users_organizations",
]


def _build_copy_into_asset(table: str) -> dg.AssetsDefinition:
    schema = _dms_schema_from_env()
    dest_table = f"{_TABLE_PREFIX}{table}"
    qualified_table = f"{_AWS_DB}.{schema}.{dest_table}"
    stage_path = f"@{_AWS_DB}.{schema}.{_STAGE_NAME}/{_STAGE_PREFIX}/{table}/"
    file_format = f"{_AWS_DB}.{schema}.dagster_plus_dms_parquet_format"

    @dg.asset(
        key=dg.AssetKey([_AWS_DB, schema, dest_table]),
        deps=[dg.AssetKey([_AWS_DB, schema, _STAGE_NAME])],
        group_name="dagster_plus_dms",
        automation_condition=dg.AutomationCondition.eager(),
        description=(
            f"Loads DMS-replicated `public.{table}` parquet files from the "
            f"{_STAGE_NAME} stage into {qualified_table} via COPY INTO. Snowflake "
            "tracks loaded files natively (FORCE = FALSE), so each file is ingested "
            "exactly once across re-runs."
        ),
    )
    def _copy_into_dms_table(
        context: dg.AssetExecutionContext,
        snowflake: SnowflakeResource,
    ) -> dg.MaterializeResult:
        with snowflake.get_connection() as conn:
            cursor = conn.cursor()

            # The AWS database requires the AWS_WRITER role for DDL and COPY INTO.
            cursor.execute(f"USE ROLE {_ROLE};")
            cursor.execute(f"USE DATABASE {_AWS_DB.upper()};")
            cursor.execute(f"USE SCHEMA {_AWS_DB.upper()}.{schema.upper()};")

            # Create the destination table from the parquet schema on first run.
            # SHOW first so we only pay for INFER_SCHEMA once, not every refresh.
            cursor.execute(f"SHOW TABLES LIKE '{dest_table}' IN SCHEMA {_AWS_DB}.{schema};")
            if not cursor.fetchall():
                # INFER_SCHEMA requires a named file format.
                cursor.execute(f"CREATE FILE FORMAT IF NOT EXISTS {file_format} TYPE = PARQUET;")
                cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {qualified_table} USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION => '{stage_path}',
                                FILE_FORMAT => '{file_format}',
                                MAX_FILE_COUNT => 50
                            )
                        )
                    );
                """)
                context.log.info(f"Created {qualified_table}")

            # COPY INTO with column-name matching so the parquet columns map onto
            # the inferred table columns regardless of order. Already-loaded files
            # are skipped automatically (FORCE = FALSE is the default).
            cursor.execute(f"""
                COPY INTO {qualified_table}
                    FROM '{stage_path}'
                    FILE_FORMAT = (TYPE = PARQUET)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
            """)
            # COPY INTO returns one row per file processed; column index 3 is
            # rows_loaded. The "0 files processed" case returns a single status
            # row without that column, so guard the access.
            rows_loaded = sum(
                row[3] for row in cursor.fetchall() if len(row) > 3 and isinstance(row[3], int)
            )
            context.log.info(f"COPY INTO {qualified_table}: {rows_loaded} new rows loaded")

        return dg.MaterializeResult(metadata={"rows_loaded": dg.MetadataValue.int(rows_loaded)})

    return _copy_into_dms_table


dagster_plus_dms_table_assets = [_build_copy_into_asset(table) for table in _DMS_TABLES]


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=dagster_plus_dms_table_assets)
