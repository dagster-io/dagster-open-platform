import os

from dagster import (
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    asset,
    get_dagster_logger,
    multi_asset,
)
from dagster_open_platform.aws.assets import workspace_data_json
from dagster_open_platform.aws.constants import BUCKET_NAME, OUTPUT_PREFIX
from dagster_snowflake import SnowflakeResource

log = get_dagster_logger()


@asset(
    name="inactive_snowflake_clones",
    description="Drops clone purina databases after 14 days of inactivity.",
)
def inactive_snowflake_clones(snowflake_sf: SnowflakeResource) -> MaterializeResult:
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
                database_name regexp $$PURINA_CLONE_\d+$$
                and days_since_last_activity > 14;        
        """)
        result = cur.fetch_pandas_all()
        dbs_to_drop = result["DATABASE_NAME"].to_list()
        if dbs_to_drop:
            for db in dbs_to_drop:
                pr_id = db.split("_")[-1]  # Get the pull request ID from the database name
                log.info(f"Dropping {db}")
                cur.execute(f"CALL UTIL_DB.PUBLIC.CLEANUP_PURINA_CLONE('{pr_id}')")
                log.info(f"{db} dropped.")
        else:
            log.info("No databases to drop.")
    return MaterializeResult(
        metadata={"dropped_databases": dbs_to_drop, "dropped_databases_count": len(dbs_to_drop)},
    )


@multi_asset(
    group_name="aws_stages",
    description="Snowflake stages for AWS data, creates new stages for new assets, refreses existing stages.",
    specs=[
        AssetSpec(
            key=[
                "aws",
                os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", ""),
                f"workspace_staging_{asset_key[0][-1]!s}",
            ],
            deps=[asset_key],
        )
        for asset_key in workspace_data_json.keys
    ],
)
def aws_stages(context: AssetExecutionContext, snowflake_sf: SnowflakeResource):
    integration_prefix = (
        "CLOUD_PROD"
        if os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", "") == "cloud-prod"
        else "DOGFOOD"
    )
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            stage_name = key[0][-1]
            object_name = stage_name.replace("workspace_staging_", "")
            cur.execute(f"USE SCHEMA AWS.{os.getenv('AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME')};")

            create_stage_query = f"""
                CREATE OR REPLACE STAGE {stage_name}
                URL='s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{object_name}'
                STORAGE_INTEGRATION = {integration_prefix}_WORKSPACE_REPLICATION
                FILE_FORMAT = AWS.PUBLIC.JSON_NO_EXTENSION
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


@multi_asset(
    group_name="aws_external_tables",
    description="Snowflake external tables for AWS data.",
    specs=[
        AssetSpec(
            key=[
                "aws",
                os.getenv("AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME", ""),
                f"{asset_key[0][-1]!s}_ext",
            ],
            deps=[asset_key],
        )
        for asset_key in aws_stages.keys
    ],
)
def aws_external_tables(context: AssetExecutionContext, snowflake_sf: SnowflakeResource):
    with snowflake_sf.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("USE ROLE AWS_WRITER;")
        for key in context.selected_asset_keys:
            table_name = key[0][-1]
            stage_name = table_name[:-4]  # Remove the "_ext" suffix
            cur.execute(f"USE SCHEMA AWS.{os.getenv('AWS_WORKSPACE_REPLICATION_ACCOUNT_NAME')};")

            create_table_query = f"""
                CREATE EXTERNAL TABLE {table_name}(
                    FILENAME VARCHAR AS METADATA$FILENAME
                )
                LOCATION = @{stage_name}
                FILE_FORMAT = AWS.PUBLIC.JSON_NO_EXTENSION
                AUTO_REFRESH = FALSE
                COMMENT = 'External table for stage {stage_name} from workspace replication';
            """
            cur.execute(f"SHOW TABLES LIKE '{table_name}';")
            tables = cur.fetchall()
            if not tables:
                cur.execute(create_table_query)
                log.info(f"Created external table {table_name}")
                continue
            cur.execute(f"ALTER EXTERNAL TABLE {table_name} REFRESH;")
            log.info(f"Refreshed external table {table_name}")
