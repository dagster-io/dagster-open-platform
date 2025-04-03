import dagster as dg
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_snowflake import SnowflakeResource

log = dg.get_dagster_logger()


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
