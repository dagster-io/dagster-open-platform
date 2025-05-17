import os
from collections.abc import Iterator
from typing import Union

import dagster as dg
import pandas as pd
from dagster.components import definitions
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_slack import SlackResource
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas


@dg.asset(
    key_prefix=["slack", "dagster"],
    group_name="slack",
    check_specs=[
        dg.AssetCheckSpec("unique_ds_check", asset=["slack", "dagster", "member_metrics"])
    ],
    description="Slack Stats, which includes number of members by day",
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),
)
def member_metrics(
    slack: SlackResource, snowflake_sf: SnowflakeResource
) -> Iterator[Union[dg.MaterializeResult, dg.AssetCheckResult]]:
    client = slack.get_client()
    # The Dagster Slack resource doesn't support setting headers
    client.headers = {"cookie": os.getenv("SLACK_ANALYTICS_COOKIE")}
    data = {
        "date_range": "28d",
        "set_active": "true",
    }
    response = client.api_call("team.stats.timeSeries", data=data).data
    assert isinstance(response, dict)
    response = response.get("stats", [])
    slack_stats = pd.DataFrame(response)
    slack_stats["ds"] = pd.to_datetime(slack_stats["ds"])

    # Add the asset check for uniqueness of the 'ds' column
    unique_ds_check_result = dg.AssetCheckResult(
        check_name="unique_ds_check",
        passed=slack_stats["ds"].is_unique,
        metadata={
            "num_unique_ds": slack_stats["ds"].nunique(),
            "total_rows": len(slack_stats),
        },
    )
    yield unique_ds_check_result

    database = get_database_for_environment("SLACK")
    schema = get_schema_for_environment("DAGSTER")
    table_name = "MEMBER_METRICS"

    with snowflake_sf.get_connection() as conn:
        # Create a temporary table to stage the new data
        temp_table_name = f"{table_name}_TEMP"
        write_pandas(
            conn=conn,
            df=slack_stats,
            table_name=temp_table_name,
            database=database,
            schema=schema,
            overwrite=True,
            auto_create_table=True,
            quote_identifiers=False,
        )

        create_table_sql = f"""
        CREATE TABLE {database}.{schema}.{table_name} IF NOT EXISTS LIKE {database}.{schema}.{temp_table_name}
        """
        conn.cursor().execute(create_table_sql)

        # Merge the temporary table with the target table
        columns = slack_stats.columns
        columns = [
            col for col in columns if col != "ENTERPRISE_ID"
        ]  # This is an empty column which causes type issues

        update_set_clause = ", ".join([f"target.{col} = source.{col}" for col in columns])
        insert_columns = ", ".join(columns)
        insert_values = ", ".join([f"source.{col}" for col in columns])

        merge_sql = f"""
        MERGE INTO {database}.{schema}.{table_name} AS target
        USING {database}.{schema}.{temp_table_name} AS source
        ON target.ds = source.ds
        WHEN MATCHED THEN
            UPDATE SET {update_set_clause}
        WHEN NOT MATCHED THEN
            INSERT ({insert_columns}) VALUES ({insert_values});
        """
        conn.cursor().execute(merge_sql)

        # Drop the temporary table
        conn.cursor().execute(f"DROP TABLE {database}.{schema}.{temp_table_name}")

        yield dg.MaterializeResult(
            metadata={"num_rows": len(slack_stats)},
        )


@definitions
def defs():
    return dg.Definitions(
        assets=[member_metrics],
        resources={
            "slack": SlackResource(token=dg.EnvVar("SLACK_ANALYTICS_TOKEN")),
        },
    )
