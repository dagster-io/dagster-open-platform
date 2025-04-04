import dagster as dg
from dagster_open_platform.utils.environment_helpers import get_environment
from dagster_snowflake import SnowflakeResource

log = dg.get_dagster_logger()

objects = ["activities", "community_members", "groups"]


@dg.multi_asset(
    group_name="aws_stages",
    description="Snowflake stages for Common Room export data.",
    can_subset=True,
    specs=[
        dg.AssetSpec(
            key=[
                "aws",
                "elementl",
                f"stage_common_room_{_object}",
            ],
            automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
        )
        for _object in objects
    ],
)
def common_room_aws_stage(context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource):
    mapping = {
        "stage_common_room_activities": "Activity",
        "stage_common_room_community_members": "CommunityMember",
        "stage_common_room_groups": "Group",
    }
    schema = "ELEMENTL" if get_environment() == "PROD" else "DEV"
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            stage_name = key.path[-1]

            # pull out the name of the AWS stage directory from the name of the stage itself
            directory = mapping.get(stage_name)
            cur.execute(f"USE SCHEMA AWS.{schema};")

            create_stage_query = f"""
                CREATE STAGE {stage_name}
                URL='s3://common-room-shared-bucket20250328005107552900000001/data/{directory}'
                STORAGE_INTEGRATION = "common-room-shared-bucket20250328005107552900000001"
                DIRECTORY = (ENABLE = TRUE)
                FILE_FORMAT = (
                    TYPE = 'JSON'
                    STRIP_OUTER_ARRAY = TRUE
                    FILE_EXTENSION = '.jsonl'
                );
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
    description="Snowflake external table for Common Room export data.",
    can_subset=True,
    specs=[
        dg.AssetSpec(
            key=[
                "aws",
                "elementl",
                f"ext_common_room_{_object}",
            ],
            deps=[["aws", "elementl", f"stage_common_room_{_object}"]],
            automation_condition=dg.AutomationCondition.on_cron("0 3 * * *"),
        )
        for _object in objects
    ],
)
def common_room_aws_external_table(
    context: dg.AssetExecutionContext, snowflake_sf: SnowflakeResource
):
    schema = "ELEMENTL" if get_environment() == "PROD" else "DEV"
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            table_name = key.path[-1]
            stage_name = table_name.replace("ext_", "stage_")
            cur.execute(f"USE SCHEMA AWS.{schema};")

            create_table_query = f"""
                CREATE OR REPLACE EXTERNAL TABLE {table_name}(
                    FILENAME VARCHAR AS METADATA$FILENAME,
                    REPLICATION_DATE DATE AS cast(
                        split_part(METADATA$FILENAME, '/', 3) || '-' ||
                        split_part(METADATA$FILENAME, '/', 4) || '-' ||
                        split_part(METADATA$FILENAME, '/', 5)
                        as date
                    )
                )
                PARTITION BY (REPLICATION_DATE)
                LOCATION = @{stage_name}
                FILE_FORMAT = (
                    TYPE = 'JSON'
                    STRIP_OUTER_ARRAY = TRUE
                    FILE_EXTENSION = '.jsonl'
                )
                PATTERN = '.*[.]jsonl'
                AUTO_REFRESH = FALSE
                REFRESH_ON_CREATE = TRUE
                COMMENT = 'External table for stage {stage_name}';
            """
            cur.execute(f"SHOW TABLES LIKE '{table_name}';")
            tables = cur.fetchall()
            if not tables:
                cur.execute(create_table_query)
                log.info(f"Created external table {table_name}")
            cur.execute(f"ALTER EXTERNAL TABLE {table_name} REFRESH;")
            log.info(f"Refreshed external table {table_name}")
