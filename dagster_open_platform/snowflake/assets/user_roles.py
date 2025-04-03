import os

import dagster as dg
from dagster_open_platform.aws.constants import BUCKET_NAME
from dagster_snowflake import SnowflakeResource

log = dg.get_dagster_logger()


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
