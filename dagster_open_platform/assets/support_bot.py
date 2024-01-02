import os

from dagster import (
    AssetExecutionContext,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    MaterializeResult,
    MonthlyPartitionsDefinition,
    asset,
)

from ..resources.scoutos_resource import GithubResource, ScoutosResource


def extract_comments(issue_or_disccusion: dict) -> str:
    comments = ""
    for comment in issue_or_disccusion["comments"]["nodes"]:
        comments += comment["bodyText"] + "\n"
    return comments


START_TIME = "2023-01-01"
monthly_partition = MonthlyPartitionsDefinition(start_date=START_TIME)
materialize_on_cron_policy = AutoMaterializePolicy.eager().with_rules(
    AutoMaterializeRule.materialize_on_cron("0 4 * * *"),
)


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
        "url": i["url"],
        "labels": ",".join([label["name"] for label in i["labels"]["nodes"]]) or "None",
        "votes": i["reactions"]["totalCount"],
    }


@asset(
    compute_kind="github",
    group_name="support_bot",
    partitions_def=monthly_partition,
    auto_materialize_policy=materialize_on_cron_policy,
    op_tags={"team": "devrel"},
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
    resp = scoutos.write_files(collection, parsed_issues)
    context.log.debug(resp)
    resp = scoutos.write_files(collection, parsed_discussions)
    context.log.debug(resp)
    return MaterializeResult(
        metadata={"issues": len(parsed_issues), "discussions": len(parsed_discussions)}
    )
