import os
from datetime import date, timedelta

import dagster as dg
from dagster.components import definitions
from dagster_open_platform.definitions import global_freshness_policy
from dagster_open_platform.defs.aws.assets import workspace_data_json
from dagster_open_platform.defs.aws.constants import BUCKET_NAME, OUTPUT_PREFIX
from dagster_snowflake import SnowflakeResource

log = dg.get_dagster_logger()


@dg.multi_asset(
    group_name="aws_stages",
    description="Snowflake stages for AWS data, creates new stages for new assets, refreses existing stages.",
    specs=[
        dg.AssetSpec(
            key=[
                "aws",
                "cloud-prod",
                f"workspace_staging_{asset_key.path[-1]!s}",
            ],
            deps=[asset_key],
            automation_condition=dg.AutomationCondition.on_cron("@daily")
            & (
                ~dg.AutomationCondition.any_deps_match(
                    dg.AutomationCondition.in_latest_time_window()
                    & dg.AutomationCondition.missing()
                )
            ),
            freshness_policy=global_freshness_policy,
        )
        for asset_key in workspace_data_json.keys
    ],
)
def workspace_replication_aws_stages(
    context: dg.AssetExecutionContext, snowflake: SnowflakeResource
):
    integration_prefix = (
        "CLOUD_PROD"
        if os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", "") == "cloud-prod"
        else "DOGFOOD"
    )
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            stage_name = key.path[-1]
            object_name = stage_name.replace("workspace_staging_", "")
            cur.execute(
                f"USE SCHEMA AWS.{os.getenv('AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME', '').replace('-', '_')};"
            )

            create_stage_query = f"""
                CREATE STAGE {stage_name}
                URL='s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{object_name}'
                STORAGE_INTEGRATION = {integration_prefix}_WORKSPACE_REPLICATION
                FILE_FORMAT = (TYPE = 'JSON', COMPRESSION = 'AUTO', STRIP_OUTER_ARRAY = TRUE)
                DIRECTORY = (ENABLE = TRUE);
            """
            cur.execute(f"SHOW STAGES LIKE '{stage_name}';")
            stages = cur.fetchall()
            if not stages:
                cur.execute(create_stage_query)
                log.info(f"Created stage {stage_name}")
                continue

            # Refresh only the last month's worth of data
            today = date.today()
            thirty_days_ago = today - timedelta(days=30)
            current_date = thirty_days_ago

            while current_date <= today:
                date_str = current_date.strftime("%Y-%m-%d")
                date_path = f"{date_str}/"
                refresh_query = f"ALTER STAGE {stage_name} REFRESH SUBPATH = '{date_path}';"
                cur.execute(refresh_query)
                log.info(f"Stage {stage_name} refreshed for path {date_path}")
                current_date += timedelta(days=1)


@dg.multi_asset(
    group_name="aws_external_tables",
    description="Snowflake external tables for AWS data.",
    specs=[
        dg.AssetSpec(
            key=[
                "aws",
                "cloud_prod",
                f"{asset_key.path[-1]!s}_ext",
            ],
            deps=[asset_key],
            automation_condition=dg.AutomationCondition.on_cron("@daily")
            & (
                ~dg.AutomationCondition.any_deps_match(
                    dg.AutomationCondition.in_latest_time_window()
                    & dg.AutomationCondition.missing()
                )
            ),
            freshness_policy=global_freshness_policy,
        )
        for asset_key in workspace_replication_aws_stages.keys
    ],
)
def workspace_replication_aws_external_tables(
    context: dg.AssetExecutionContext, snowflake: SnowflakeResource
):
    with snowflake.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            table_name = key.path[-1]
            stage_name = table_name[:-4]  # Remove the "_ext" suffix
            cur.execute(
                f"USE SCHEMA AWS.{os.getenv('AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME', '').replace('-', '_')};"
            )

            create_table_query = f"""
                CREATE OR REPLACE EXTERNAL TABLE {table_name}(
                    FILENAME VARCHAR AS METADATA$FILENAME,
                    REPLICATION_DATE DATE AS cast(split_part(METADATA$FILENAME, '/', 3) as date),
                    ORGANIZATION_ID VARCHAR AS split_part(METADATA$FILENAME, '/', 4),
                    DEPLOYMENT_ID VARCHAR AS split_part(METADATA$FILENAME, '/', 5),
                    CODE_LOCATION VARCHAR AS replace(split_part(METADATA$FILENAME, '/', 6), '.json')
                )
                PARTITION BY (REPLICATION_DATE)
                LOCATION = @{stage_name}
                FILE_FORMAT = (TYPE = 'JSON', COMPRESSION = 'AUTO', STRIP_OUTER_ARRAY = TRUE)
                AUTO_REFRESH = FALSE
                COMMENT = 'External table for stage {stage_name} from workspace replication';
            """
            cur.execute(f"SHOW EXTERNAL TABLES LIKE '{table_name}';")
            tables = cur.fetchall()
            if not tables:
                cur.execute(create_table_query)
                log.info(f"Created external table {table_name}")
                continue

            # Refresh only the last month's worth of data
            today = date.today()
            thirty_days_ago = today - timedelta(days=30)
            current_date = thirty_days_ago

            while current_date <= today:
                date_str = current_date.strftime("%Y-%m-%d")
                date_path = f"{date_str}/"
                refresh_query = f"ALTER EXTERNAL TABLE {table_name} REFRESH '{date_path}';"
                cur.execute(refresh_query)
                log.info(f"External table {table_name} refreshed for path {date_path}")
                current_date += timedelta(days=1)


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[workspace_replication_aws_stages, workspace_replication_aws_external_tables]
    )
