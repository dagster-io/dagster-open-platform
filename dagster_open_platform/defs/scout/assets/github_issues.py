import os

import dagster as dg
from dagster.components import definitions
from dagster_open_platform.defs.scout.resources.github_resource import GithubResource
from dagster_open_platform.defs.scout.resources.scoutos_resource import ScoutosResource


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


@definitions
def defs():
    return dg.Definitions(
        assets=[github_issues],
    )
