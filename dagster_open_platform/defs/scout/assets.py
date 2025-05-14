import json
import os

import dagster as dg
import pandas as pd
from dagster_open_platform.defs.scout.resources import GithubResource, ScoutosResource
from dagster_open_platform.utils.environment_helpers import (
    get_database_for_environment,
    get_schema_for_environment,
)
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas


def extract_comments(issue_or_disccusion: dict) -> str:
    comments = ""
    for comment in issue_or_disccusion["comments"]["nodes"]:
        comments += comment["bodyText"] + "\n"
    return comments


START_TIME = "2023-01-01"
daily_partition = dg.DailyPartitionsDefinition(start_date=START_TIME)


def parse_discussion(d: dict) -> dict:
    answer = d["answer"]["bodyText"] if d["answer"] else "UNANSWERED"
    text = (
        f"DISCUSSION TITLE: {d['title']}\n" + f"QUESTION: {d['bodyText']}\n" + f"ANSWER: {answer}\n"
    ).strip()
    return {
        "_key": d["id"],
        "type": "text",
        "content": text,
        "document_type": "discussion",
        "title": d["title"],
        "category": d["category"].get("name", "Uncategorized"),
        "created_at": d["createdAt"],
        "cm8iylanm1rsa09s6i0br86xa": d[
            "closed"
        ],  # column has been renamed in API to be this unique identifier
        "state_reason": d["stateReason"],
        "url": d["url"],
        "labels": ",".join([label["name"] for label in d["labels"]["nodes"]]) or "None",
        "votes": d["reactions"]["totalCount"],
    }


def parse_issue(i: dict) -> dict:
    text = (
        f"ISSUE TITLE: {i['title']}\n"
        + f"BODY: {i['bodyText']}\n---\n"
        + f"COMMENTS: {extract_comments(i)}"
    ).strip()
    return {
        "_key": i["id"],
        "type": "text",
        "content": text,
        "document_type": "issue",
        "title": i["title"],
        "created_at": i["createdAt"],
        "closed_at": i["closedAt"],
        "state": i["state"],
        "state_reason": i["stateReason"],
        "url": i["url"],
        "labels": ",".join([label["name"] for label in i["labels"]["nodes"]]) or "None",
        "votes": i["reactions"]["totalCount"],
    }


@dg.asset(
    group_name="support_bot",
    partitions_def=daily_partition,
    backfill_policy=dg.BackfillPolicy.single_run(),
    kinds={"github", "scout"},
    owners=["team:devrel"],
)
def github_issues(
    context: dg.AssetExecutionContext, github: GithubResource, scoutos: ScoutosResource
) -> dg.MaterializeResult:
    """Fetch Github Issues and Discussions and feed into Scout Support Bot.

    Since the Github API limits search results to 1000, we partition by updated at
    month, which should be enough to get all the issues and discussions. We use
    updated_at to ensure we don't miss any issues that are updated after the
    partition month. The underlying auto-materialize policy runs this asset every
    day to refresh all data for the current month.
    """
    start, end = context.partition_time_window
    context.log.info(f"Finding issues from {start} to {end}")

    issues = github.get_issues(
        start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d")
    )
    context.log.info(f"Found {len(issues)} issues")

    parsed_issues = [parse_issue(i) for i in issues]
    context.log.info(f"Found {len(parsed_issues)} parsed issues")

    discussions = github.get_discussions(
        start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d")
    )
    context.log.info(f"Found {len(discussions)} discussions")
    parsed_discussions = [parse_discussion(d) for d in discussions]
    context.log.info(f"Found {len(parsed_discussions)} parsed discussions")
    collection_id = os.getenv("SCOUTOS_COLLECTION_ID", "")
    table_id = os.getenv("SCOUTOS_TABLE_ID", "")
    context.log.info(f"Using Collection ID: {collection_id}")
    scoutos.write_documents(collection_id, table_id, parsed_issues)
    scoutos.write_documents(collection_id, table_id, parsed_discussions)
    return dg.MaterializeResult()


scout_queries_daily_partition = dg.DailyPartitionsDefinition(start_date="2024-06-01")


@dg.asset(
    partitions_def=scout_queries_daily_partition,
    group_name="scoutos",
    description="ScoutOS App Runs",
    automation_condition=dg.AutomationCondition.on_cron("0 0 * * *"),
    kinds={"github", "scout", "snowflake"},
    owners=["team:devrel"],
    tags={"dagster/concurrency_key": "scoutos_app_runs"},
)
def scoutos_app_runs(
    context: dg.AssetExecutionContext, snowflake: SnowflakeResource, scoutos: ScoutosResource
) -> dg.MaterializeResult:
    start, end = context.partition_time_window
    data = scoutos.get_runs(start, end)

    df = pd.concat([pd.json_normalize(obj) for obj in data], ignore_index=True)
    df["partition_date"] = df["timestamp_start"].str[:10]
    context.log.info(f"Fetched Total records: {len(df)}")

    df["block_id"] = df["blocks"].apply(
        lambda x: [block["block_id"] for block in x if "block_id" in block]
    )
    df["blocks"] = df["blocks"].apply(
        lambda x: [block["block_output"] for block in x if "block_output" in block]
    )
    df["blocks"] = df["blocks"].apply(lambda x: json.dumps(x) if x is not None else None)
    database_name = get_database_for_environment("SCOUT")
    schema_name = get_schema_for_environment("QUERIES")
    table_name = "scout_queries"
    temp_table_name = f"{table_name}_TEMP"
    with snowflake.get_connection() as conn:
        create_schema_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {database_name}.{schema_name}
        """
        conn.cursor().execute(create_schema_sql)

        write_pandas(
            conn,
            df,
            temp_table_name,
            database=database_name,
            schema=schema_name,
            overwrite=True,
            auto_create_table=True,
            quote_identifiers=False,
        )

        create_table_sql = f"""
        CREATE TABLE {database_name}.{schema_name}.{table_name} IF NOT EXISTS LIKE {database_name}.{schema_name}.{temp_table_name}
        """
        conn.cursor().execute(create_table_sql)

        delete_query = f"""
            delete from {database_name}.{schema_name}.{table_name}
            where partition_date ='{start.strftime("%Y-%m-%d")}'
        """

        try:
            conn.cursor().execute(delete_query)
            context.log.info(f"Deleted Partition data for {start}")

            success, number_chunks, rows_inserted, output = write_pandas(
                conn,
                df,
                table_name,
                database=database_name,
                schema=schema_name,
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
            )

            context.log.info(
                f"Inserted {rows_inserted} rows into {database_name}.{schema_name}.{table_name}"
            )
        except Exception as e:
            context.log.error(f"Error inserting data into {table_name}")
            context.log.error(e)
            conn.rollback()
            raise e

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(rows_inserted),
            "preview": dg.MetadataValue.md(df.head(10).to_markdown(index=False)),
        }
    )
