from datetime import UTC, date, datetime, timedelta

import dagster as dg
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_snowflake import SnowflakeResource

log = dg.get_dagster_logger()

DATABASE = "AWS"
STAGE_NAME = "streamline_events_stage"
TABLE_NAME = "streamline_events"
S3_PREFIX = "streamline-events"
# The streamline event stream lands a large volume of parquet files each hour;
# run the COPY INTO on the bigger warehouse (same as the large DMS tables).
WAREHOUSE = "L_WAREHOUSE"
# Listing every file under the prefix exceeds Snowflake's 1 GB stage-listing limit,
# so each COPY INTO only lists files landed in the last N UTC days.
LOAD_LOOKBACK_DAYS = 10


def _bucket_and_schema() -> tuple[str, str]:
    """Bucket (and identically-named Snowflake storage integration) plus target schema.

    The prod deployment reads from the cloud-prod AWS account; every other
    environment reads from the dogfood resources (same convention as DMS).
    """
    if get_environment() == "PROD":
        return "cloud-prod-streamline-event-stream", "CLOUD_PROD"
    return "dogfood-streamline-event-stream", "DEV"


def _recent_files_pattern(today: date, days: int = LOAD_LOOKBACK_DAYS) -> str:
    """Regex for parquet files landed in the last ``days`` UTC days.

    Keys are ``{org_id}/{deployment_id}/YYYY/MM/DD/HH/{position}.parquet``.
    """
    dates = "|".join((today - timedelta(days=n)).strftime("%Y/%m/%d") for n in range(days))
    return f".*/({dates})/.*[.]parquet"


@dg.asset(
    group_name="aws_stages",
    description="Snowflake stage over the streamline event stream parquet files in S3.",
    key=["aws", "cloud_prod", STAGE_NAME],
    automation_condition=dg.AutomationCondition.on_cron("0 * * * *"),
)
def streamline_events_aws_stage(
    context: dg.AssetExecutionContext, snowflake: SnowflakeResource
) -> None:
    bucket, schema = _bucket_and_schema()
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        cur.execute(f"USE SCHEMA {DATABASE}.{schema};")

        # No DIRECTORY table: the downstream COPY INTO lists stage files itself,
        # so there is nothing to refresh on subsequent runs.
        cur.execute(f"""
            CREATE STAGE IF NOT EXISTS {STAGE_NAME}
            URL='s3://{bucket}/{S3_PREFIX}/'
            STORAGE_INTEGRATION = "{bucket}"
            FILE_FORMAT = (TYPE = 'PARQUET');
        """)
        log.info(f"Stage {DATABASE}.{schema}.{STAGE_NAME} is present")


@dg.asset(
    group_name="aws_tables",
    description=(
        "Snowflake table of streamline events, loaded incrementally every hour "
        "from the streamline event stream stage via COPY INTO."
    ),
    key=["aws", "cloud_prod", TABLE_NAME],
    deps=[streamline_events_aws_stage],
    automation_condition=dg.AutomationCondition.on_cron("0 * * * *")
    & ~dg.AutomationCondition.in_progress(),
)
def streamline_events_table(
    context: dg.AssetExecutionContext, snowflake: SnowflakeResource
) -> dg.MaterializeResult:
    _, schema = _bucket_and_schema()
    qualified_name = f"{DATABASE}.{schema}.{TABLE_NAME}"

    # Mirrors EVENT_STREAM_SCHEMA in dagster_cloud_backend.streamline.consumers.event_stream_s3
    # (landed_at is appended at flush time).
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            EVENT_LOG_ID NUMBER,
            ORGANIZATION_ID NUMBER,
            DEPLOYMENT_ID NUMBER,
            RUN_ID VARCHAR,
            DAGSTER_EVENT_TYPE VARCHAR,
            ASSET_KEY VARCHAR,
            STEP_KEY VARCHAR,
            PARTITION VARCHAR,
            EVENT_DATA VARCHAR,
            CREATED_AT TIMESTAMP_TZ,
            LANDED_AT TIMESTAMP_TZ
        );
    """

    # COPY INTO skips files already recorded in the table's load metadata,
    # so overlapping days across hourly runs don't load twice.
    pattern = _recent_files_pattern(datetime.now(UTC).date())
    copy_query = f"""
        COPY INTO {TABLE_NAME}
        FROM @{STAGE_NAME}
        PATTERN = '{pattern}'
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
    """

    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        cur.execute(f"USE WAREHOUSE {WAREHOUSE};")
        cur.execute(f"USE SCHEMA {DATABASE}.{schema};")
        cur.execute(create_table_query)
        cur.execute(copy_query)
        copy_results = cur.fetchall()

        files_loaded = 0
        rows_loaded = 0
        for row in copy_results:
            # Each loaded file yields (file, status, rows_parsed, rows_loaded, ...);
            # a run with no new files yields a single status-message row.
            if len(row) >= 4 and row[1] == "LOADED":
                files_loaded += 1
                rows_loaded += int(row[3])
        log.info(f"Loaded {rows_loaded} rows from {files_loaded} files into {qualified_name}")

        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME};")
        total_rows = cur.fetchone()[0]  # type: ignore

    return dg.MaterializeResult(
        metadata={
            "snowflake_table": qualified_name,
            "files_loaded": files_loaded,
            "rows_loaded": rows_loaded,
            "total_rows": total_rows,
            "copy_pattern": pattern,
        }
    )
