import os

import dagster as dg
from dagster_open_platform.aws.assets import workspace_data_json
from dagster_open_platform.aws.constants import BUCKET_NAME, OUTPUT_PREFIX
from dagster_snowflake import SnowflakeResource

from ..utils.environment_helpers import get_environment

log = dg.get_dagster_logger()


@dg.asset(
    name="inactive_snowflake_clones",
    description="Drops clone purina databases after 14 days of inactivity.",
)
def inactive_snowflake_clones(snowflake_sf: SnowflakeResource) -> dg.MaterializeResult:
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute(r"""
            with
            recent_queries as (
                select
                    database_name,
                    coalesce(
                        max(date(start_time)),
                        current_date - 30
                    ) as last_query_date
                from snowflake.account_usage.query_history
                where date(start_time) > current_date - 30
                group by all
            )
            select
                database_name,
                greatest(
                    date(created),
                    date(last_altered),
                    coalesce(last_query_date, current_date - 30)
                ) as last_activity,
                current_date - last_activity as days_since_last_activity
            from snowflake.information_schema.databases
                left join recent_queries using(database_name)
            where
                database_name regexp $$\w+_CLONE_\d+$$
                and days_since_last_activity > 14;
        """)
        result = cur.fetch_pandas_all()
        dbs_to_drop = result["DATABASE_NAME"].to_list()
        if dbs_to_drop:
            for db in dbs_to_drop:
                pr_id = db.split("_")[-1]  # Get the pull request ID from the database name
                base_db_name = "_".join(db.split("_")[:-2])
                log.info(f"Dropping {db}")
                cur.execute(
                    f"CALL UTIL_DB.PUBLIC.CLEANUP_DATABASE_CLONE('{base_db_name}', '{pr_id}')"
                )
                log.info(f"{db} dropped.")
        else:
            log.info("No databases to drop.")
    return dg.MaterializeResult(
        metadata={"dropped_databases": dbs_to_drop, "dropped_databases_count": len(dbs_to_drop)},
    )


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
            automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
        )
        for asset_key in workspace_data_json.keys
    ],
)
def workspace_replication_aws_stages(
    context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource
):
    integration_prefix = (
        "CLOUD_PROD"
        if os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", "") == "cloud-prod"
        else "DOGFOOD"
    )
    with snowflake_sf.get_connection() as conn:
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
            cur.execute(f"ALTER STAGE {stage_name} REFRESH;")
            log.info(f"Stage {stage_name} refreshed")


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
            automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
        )
        for asset_key in workspace_replication_aws_stages.keys
    ],
)
def workspace_replication_aws_external_tables(
    context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource
):
    with snowflake_sf.get_connection() as conn:
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
            cur.execute(create_table_query)
            log.info(f"Created external table {table_name}")


@dg.asset(
    group_name="aws_stages",
    description="Snowflake stages for AWS data, creates new stages for new assets, refreses existing stages.",
    key=["aws", "cloud-prod", "user_roles"],
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
)
def user_roles_aws_stage(context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource):
    integration_prefix = (
        "CLOUD_PROD"
        if os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", "") == "cloud-prod"
        else "DOGFOOD"
    )
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            stage_name = key.path[-1]
            cur.execute(
                f"USE SCHEMA AWS.{os.getenv('AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME', '').replace('-', '_')};"
            )

            create_stage_query = f"""
                CREATE STAGE {stage_name}
                URL='s3://{BUCKET_NAME}/raw/{stage_name}'
                STORAGE_INTEGRATION = {integration_prefix}_WORKSPACE_REPLICATION
                FILE_FORMAT = 'JSON_NO_EXTENSION'
                DIRECTORY = (ENABLE = TRUE);
            """
            cur.execute(f"SHOW STAGES LIKE '{stage_name}';")
            stages = cur.fetchall()
            if not stages:
                cur.execute(create_stage_query)
                log.info(f"Created stage {stage_name}")
                continue
            cur.execute(f"ALTER STAGE {stage_name} REFRESH;")
            log.info(f"Stage {stage_name} refreshed")


@dg.asset(
    group_name="aws_external_tables",
    description="Snowflake external tables for AWS data.",
    key=["aws", "cloud_prod", "user_roles_ext"],
    deps=[user_roles_aws_stage],
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
)
def user_roles_aws_external_table(
    context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource
):
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        table_name = context.asset_key.path[-1]
        stage_name = table_name[:-4]  # Remove the "_ext" suffix
        cur.execute(
            f"USE SCHEMA AWS.{os.getenv('AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME', '').replace('-', '_')};"
        )

        create_table_query = f"""
            CREATE EXTERNAL TABLE {table_name}(
                FILENAME VARCHAR AS METADATA$FILENAME
            )
            LOCATION = @{stage_name}
            FILE_FORMAT = 'JSON_NO_EXTENSION'
            AUTO_REFRESH = FALSE
            COMMENT = 'External table for stage {stage_name} for licensed user roles';
        """
        cur.execute(f"SHOW TABLES LIKE '{table_name}';")
        tables = cur.fetchall()
        if not tables:
            cur.execute(create_table_query)
            log.info(f"Created external table {table_name}")
        cur.execute(f"ALTER EXTERNAL TABLE {table_name} REFRESH;")
        log.info(f"Refreshed external table {table_name}")


@dg.asset(
    group_name="aws_stages",
    description="Snowflake stage for open source telemetry data.",
    key=["aws", "oss_telemetry", "oss_telemetry_prod"],
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
)
def oss_telemetry_aws_stage(context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource):
    integration_suffix = "prod" if get_environment() == "PROD" else "dev"
    schema = "OSS_TELEMETRY" if get_environment() == "PROD" else "DEV"
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            stage_name = key.path[-1]
            cur.execute(f"USE SCHEMA AWS.{schema};")

            create_stage_query = f"""
                CREATE STAGE {stage_name}
                URL='s3://oss-telemetry-{integration_suffix}'
                STORAGE_INTEGRATION = "oss-telemetry-{integration_suffix}"
                FILE_FORMAT = 'JSON_NO_EXTENSION'
                DIRECTORY = (ENABLE = TRUE);
            """
            cur.execute(f"SHOW STAGES LIKE '{stage_name}';")
            stages = cur.fetchall()
            if not stages:
                cur.execute(create_stage_query)
                log.info(f"Created stage {stage_name}")
                continue
            cur.execute(f"ALTER STAGE {stage_name} REFRESH;")
            log.info(f"Stage {stage_name} refreshed")


@dg.asset(
    group_name="aws_external_tables",
    description="Snowflake external table for open source telemetry data.",
    key=["aws", "oss_telemetry", "oss_telemetry_prod_ext"],
    deps=[oss_telemetry_aws_stage],
    automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
)
def oss_telemetry_aws_external_table(
    context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource
):
    schema = "OSS_TELEMETRY" if get_environment() == "PROD" else "DEV"
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        table_name = context.asset_key.path[-1]
        stage_name = table_name[:-4]  # Remove the "_ext" suffix
        cur.execute(f"USE SCHEMA AWS.{schema};")

        create_table_query = f"""
            CREATE EXTERNAL TABLE {table_name}(
                FILENAME VARCHAR AS METADATA$FILENAME,
                REPLICATION_DATE DATE AS cast(split_part(METADATA$FILENAME, '/', 1) || '-' || split_part(METADATA$FILENAME, '/', 2) || '-' || split_part(METADATA$FILENAME, '/', 3) as date)
            )
            PARTITION BY (REPLICATION_DATE)
            LOCATION = @{stage_name}
            FILE_FORMAT = 'JSON_NO_EXTENSION'
            AUTO_REFRESH = FALSE
            COMMENT = 'External table for stage {stage_name} for open source telemetry';
        """
        cur.execute(f"SHOW TABLES LIKE '{table_name}';")
        tables = cur.fetchall()
        if not tables:
            cur.execute(create_table_query)
            log.info(f"Created external table {table_name}")
        cur.execute(f"ALTER EXTERNAL TABLE {table_name} REFRESH;")
        log.info(f"Refreshed external table {table_name}")
