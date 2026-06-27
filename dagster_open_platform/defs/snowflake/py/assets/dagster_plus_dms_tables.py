"""COPY INTO assets for the Dagster Plus DMS replication tables.

AWS DMS replicates selected tables from the cloud-prod Postgres shards to S3
(``s3://dagster-dms-cloud-prod``), exposed in Snowflake through the
``aws.<schema>.dagster_plus_dms_stage`` stage created by the ``dagster_plus_dms``
component (``defs/snowflake/components/dagster_plus_dms``). The stage points at
the bucket root, and DMS lands each table under a prefix named after the
(post-rename) source schema:

- ``public/<table>/``        - unsharded tables (shard0 ``public`` + xregion).
- ``public_shard0/<table>/`` - sharded tables replicated from shard0 (``main``).
- ``public_shard1/<table>/`` - sharded tables replicated from shard1.
- ``partitions/job_ticks_ptn_*/``        - shard0 job_ticks child partitions.
- ``partitions_shard1/job_ticks_ptn_*/`` - shard1 job_ticks child partitions.

For every replicated table this module defines one asset that loads the parquet
files for that table into a final ``aws.<schema>.dagster_plus__<table>`` table via
``COPY INTO``. Snowflake tracks loaded files natively (``FORCE = FALSE``), so each
file is ingested exactly once across re-runs.

Sharded tables land in per-shard destination tables suffixed with ``__shard0`` /
``__shard1`` (e.g. ``dagster_plus__jobs__shard0``). Shards are kept separate
because shard0 and shard1 have independent primary-key sequences, so a union
would collide; this also mirrors the legacy sling convention
(``sling.cloud_product`` / ``sling.cloud_product_shard1``).

The schema mirrors the stage (see ``_dms_schema_from_env``): ``cloud_prod`` in
prod and ``dev`` everywhere else. In production these assets therefore resolve to
keys / tables like ``aws.cloud_prod.dagster_plus__users`` and
``aws.cloud_prod.dagster_plus__jobs__shard0``.

NOTE: the cdc-only tasks (``runs``, ``run_tags``, ``job_ticks``) replicate only
changes since the DMS task started - they have no full-load baseline, so their
destination tables are a delta log rather than a full snapshot. Until DMS emits
its first file for such a table the asset is a no-op (see the empty-location
guard below); downstream models are responsible for any historical backfill.
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
# DMS writes unsharded files under a prefix named after the source schema.
_STAGE_PREFIX = "public"
_TABLE_PREFIX = "dagster_plus__"

# Unsharded tables replicated into the `public/` prefix of the DMS stage. Keep in
# sync with `main_public_tables` plus the xregion `organizations` task in
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

# Sharded tables, replicated per shard into the `public_shard0/` and
# `public_shard1/` prefixes. Keep in sync with `jobs_shard*`, `medium_shard*`,
# and `runs_shard*` tasks in
# infra/terragrunt/cloud-prod/us-west-2/cloud-prod/dms/terragrunt.hcl.
_SHARD_PREFIXES = {"shard0": "public_shard0", "shard1": "public_shard1"}

# full-load-and-cdc: a complete snapshot is present in S3.
_SHARDED_TABLES = [
    "jobs",
    "alerts",
    "audit_log",
    "notifications",
    "alert_policies",
    "asset_diff_history",
    "code_location_load_history",
]

# cdc-only: only changes since the task started (no full-load baseline).
_SHARDED_CDC_ONLY_TABLES = [
    "runs",
    "run_tags",
]

# job_ticks is partitioned; DMS lands each `job_ticks_ptn_*` child partition under
# its own subfolder of these (per-shard) prefixes. We recurse the whole prefix and
# union the child partitions into one destination table per shard.
_JOB_TICKS_PREFIXES = {"shard0": "partitions", "shard1": "partitions_shard1"}


def _build_copy_into_asset(
    *,
    stage_subpath: str,
    dest_table: str,
    description_source: str,
    warehouse: str | None = None,
) -> dg.AssetsDefinition:
    schema = _dms_schema_from_env()
    qualified_table = f"{_AWS_DB}.{schema}.{dest_table}"
    stage_path = f"@{_AWS_DB}.{schema}.{_STAGE_NAME}/{stage_subpath}/"
    file_format = f"{_AWS_DB}.{schema}.dagster_plus_dms_parquet_format"

    @dg.asset(
        key=dg.AssetKey([_AWS_DB, schema, dest_table]),
        deps=[dg.AssetKey([_AWS_DB, schema, _STAGE_NAME])],
        group_name="dagster_plus_dms",
        automation_condition=dg.AutomationCondition.eager(),
        description=(
            f"Loads DMS-replicated `{description_source}` parquet files from the "
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
            # jobs/job_ticks are large; COPY INTO them on a bigger warehouse.
            if warehouse:
                cursor.execute(f"USE WAREHOUSE {warehouse};")
            cursor.execute(f"USE DATABASE {_AWS_DB.upper()};")
            cursor.execute(f"USE SCHEMA {_AWS_DB.upper()}.{schema.upper()};")

            # Create the destination table from the parquet schema on first run.
            # SHOW first so we only pay for INFER_SCHEMA once, not every refresh.
            cursor.execute(f"SHOW TABLES LIKE '{dest_table}' IN SCHEMA {_AWS_DB}.{schema};")
            if not cursor.fetchall():
                # INFER_SCHEMA requires a named file format.
                cursor.execute(f"CREATE FILE FORMAT IF NOT EXISTS {file_format} TYPE = PARQUET;")

                # cdc-only tasks (runs, run_tags, job_ticks) may not have emitted any
                # files yet. INFER_SCHEMA over an empty location returns no columns,
                # which would make CREATE TABLE ... USING TEMPLATE fail, so skip until
                # DMS lands the first file. The table is created on a later tick.
                cursor.execute(f"""
                    SELECT COUNT(*) FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION => '{stage_path}',
                            FILE_FORMAT => '{file_format}',
                            MAX_FILE_COUNT => 50
                        )
                    );
                """)
                inferred_columns = cursor.fetchone()
                if not inferred_columns or not inferred_columns[0]:
                    context.log.info(
                        f"No parquet files under {stage_path} yet; skipping create + "
                        f"COPY INTO for {qualified_table} until DMS emits data."
                    )
                    return dg.MaterializeResult(metadata={"rows_loaded": dg.MetadataValue.int(0)})

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


# Unsharded tables: public/<table>/ -> dagster_plus__<table>
dagster_plus_dms_table_assets = [
    _build_copy_into_asset(
        stage_subpath=f"{_STAGE_PREFIX}/{table}",
        dest_table=f"{_TABLE_PREFIX}{table}",
        description_source=f"{_STAGE_PREFIX}.{table}",
    )
    for table in _DMS_TABLES
]

# Sharded tables: public_shard{N}/<table>/ -> dagster_plus__<table>__shard{N}
dagster_plus_dms_table_assets += [
    _build_copy_into_asset(
        stage_subpath=f"{prefix}/{table}",
        dest_table=f"{_TABLE_PREFIX}{table}__{shard}",
        description_source=f"{prefix}.{table}",
        warehouse="L_WAREHOUSE" if table == "jobs" else None,
    )
    for shard, prefix in _SHARD_PREFIXES.items()
    for table in (*_SHARDED_TABLES, *_SHARDED_CDC_ONLY_TABLES)
]

# job_ticks: recurse the per-shard partitions prefix (all job_ticks_ptn_* child
# partitions share one schema) -> dagster_plus__job_ticks__shard{N}
dagster_plus_dms_table_assets += [
    _build_copy_into_asset(
        stage_subpath=prefix,
        dest_table=f"{_TABLE_PREFIX}job_ticks__{shard}",
        description_source=f"{prefix}.job_ticks_ptn_*",
        warehouse="L_WAREHOUSE",
    )
    for shard, prefix in _JOB_TICKS_PREFIXES.items()
]


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=dagster_plus_dms_table_assets)
