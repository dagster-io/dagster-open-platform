import json
import os

import pandas as pd
from dagster import (
    AssetExecutionContext,
    BackfillPolicy,
    DailyPartitionsDefinition,
    MaterializeResult,
    MetadataValue,
    asset,
)
from dagster_snowflake import SnowflakeResource
from snowflake.connector.pandas_tools import write_pandas

from .resources import GithubResource, ScoutosResource


def extract_comments(issue_or_disccusion: dict) -> str:
    comments = ""
    for comment in issue_or_disccusion["comments"]["nodes"]:
        comments += comment["bodyText"] + "\n"
    return comments


START_TIME = "2023-01-01"
daily_partition = DailyPartitionsDefinition(start_date=START_TIME)


def parse_discussion(d: dict) -> dict:
    answer = d["answer"]["bodyText"] if d["answer"] else "UNANSWERED"
    text = (
        f"DISCUSSION TITLE: {d['title']}\n" + f"QUESTION: {d['bodyText']}\n" + f"ANSWER: {answer}\n"
    ).strip()
    return {
        "id": d["id"],
        "type": "text",
        "text": text,
        "document_type": "discussion",
        "title": d["title"],
        "category": d["category"].get("name", "Uncategorized"),
        "created_at": d["createdAt"],
        "closed": d["closed"],
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
        "id": i["id"],
        "type": "text",
        "text": text,
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


@asset(
    compute_kind="github",
    group_name="support_bot",
    partitions_def=daily_partition,
    op_tags={"team": "devrel"},
    backfill_policy=BackfillPolicy.single_run(),
)
def github_issues(
    context: AssetExecutionContext, github: GithubResource, scoutos: ScoutosResource
) -> MaterializeResult:
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
    collection = os.getenv("SCOUTOS_COLLECTION_ID", "")
    context.log.info(f"Using Collection ID: {collection}")
    scoutos.write_documents(collection, parsed_issues)
    scoutos.write_documents(collection, parsed_discussions)
    return MaterializeResult()


scout_queries_daily_partition = DailyPartitionsDefinition(start_date="2024-06-01")


@asset(
    compute_kind="python",
    partitions_def=scout_queries_daily_partition,
    group_name="scoutos",
)
def scoutos_app_runs(
    context: AssetExecutionContext, snowflake: SnowflakeResource, scoutos: ScoutosResource
) -> MaterializeResult:
    partition_date_str = context.partition_key
    data = scoutos.get_runs(partition_date_str, partition_date_str)
    df = pd.concat([pd.json_normalize(obj) for obj in data], ignore_index=True)
    df["partition_date"] = df["timestamp_start"].str[:10]
    df["blocks"] = df["blocks"].apply(
        lambda x: [block["block_output"] for block in x if "block_output" in block]
    )
    df["blocks"] = df["blocks"].apply(lambda x: json.dumps(x) if x is not None else None)
    context.log.info(f"Fetched Total records: {len(df)}")
    with snowflake.get_connection() as conn:
        table_name = "scout_queries"
        delete_query = f"""
            delete from SCOUT.QUERIES.{table_name}
            where partition_date ='{partition_date_str}'
        """
        try:
            conn.cursor().execute(delete_query)
            context.log.info(f"Deleted Partition data for {partition_date_str}")

            success, number_chunks, rows_inserted, output = write_pandas(
                conn,
                df,
                table_name,
                database="SCOUT",
                schema="QUERIES",
                auto_create_table=False,
                overwrite=False,
                quote_identifiers=False,
            )

            context.log.info(f"Inserted {rows_inserted} rows into SCOUT.QUERIES.{table_name}")
        except Exception as e:
            context.log.error(f"Error inserting data into {table_name}")
            context.log.error(e)
            conn.rollback()
            raise e

    return MaterializeResult(
        metadata={
            "row_count": MetadataValue.int(rows_inserted),
            "preview": MetadataValue.md(df.head(10).to_markdown(index=False)),
        }
    )
