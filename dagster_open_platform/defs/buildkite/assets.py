from collections.abc import Generator
from datetime import datetime, timedelta, timezone

import dagster as dg
from dagster import AutomationCondition
from dagster_anthropic import AnthropicResource
from dagster_slack import SlackResource
from dagster_snowflake import SnowflakeResource

from dagster_open_platform.defs.buildkite.partitions import buildkite_hourly_partition
from dagster_open_platform.defs.buildkite.prompt import BUILDKITE_ANALYSIS_SYSTEM_PROMPT
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource
from dagster_open_platform.defs.buildkite.utils import (
    BuildkiteSQL,
    build_claude_context,
    extract_failed_job_logs,
    format_slack_blocks,
    summarize_builds,
)
from dagster_open_platform.utils.environment_helpers import get_environment

PIPELINES_TO_MONITOR = ["internal"]


_buildkite_raw_automation_condition = (
    AutomationCondition.cron_tick_passed("0 * * * *")
    & ~AutomationCondition.in_progress()
    & AutomationCondition.in_latest_time_window(lookback_delta=timedelta(hours=2))
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            key="buildkite_builds",
            group_name="buildkite",
            tags={"dagster/kind/buildkite": "", "dagster/kind/snowflake": ""},
            description="One row per Buildkite build across all pipelines and branches.",
            automation_condition=_buildkite_raw_automation_condition,
        ),
        dg.AssetSpec(
            key="buildkite_jobs",
            group_name="buildkite",
            tags={"dagster/kind/buildkite": "", "dagster/kind/snowflake": ""},
            description="One row per job across all Buildkite builds.",
            automation_condition=_buildkite_raw_automation_condition,
        ),
    ],
    partitions_def=buildkite_hourly_partition,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=24),
)
def buildkite_raw(
    context: dg.AssetExecutionContext,
    buildkite: BuildkiteResource,
    snowflake: SnowflakeResource,
) -> Generator[dg.MaterializeResult]:
    """Fetches all finished Buildkite builds (and their jobs) for the partition hour
    and loads them into Snowflake. Runs every hour and re-processes the last two
    hourly windows to capture builds that were still in-flight during the previous run.
    """
    window = context.partition_time_window
    created_from = window.start.isoformat()
    created_to = window.end.isoformat()

    context.log.info(
        "Fetching finished builds created_from=%s created_to=%s",
        created_from,
        created_to,
    )
    builds = buildkite.get_builds(
        created_from=created_from,
        created_to=created_to,
        state="finished",
    )
    context.log.info("Fetched %d builds", len(builds))

    jobs = [j for b in builds for j in b.jobs]
    context.log.info("Fetched %d builds and %d jobs", len(builds), len(jobs))

    buildkite_sql = BuildkiteSQL(snowflake)

    buildkite_sql.insert_builds(builds)
    yield dg.MaterializeResult(
        asset_key="buildkite_builds",
        metadata={
            "num_builds": dg.MetadataValue.int(len(builds)),
            "partition_start": created_from,
            "partition_end": created_to,
        },
    )

    buildkite_sql.insert_jobs(jobs)
    yield dg.MaterializeResult(
        asset_key="buildkite_jobs",
        metadata={
            "num_jobs": dg.MetadataValue.int(len(jobs)),
            "partition_start": created_from,
            "partition_end": created_to,
        },
    )


class BuildkiteAnalysisConfig(dg.Config):
    force_slack_post: bool = False


@dg.asset(
    group_name="buildkite",
    tags={
        "dagster/kind/buildkite": "",
        "dagster/kind/anthropic": "",
        "dagster/kind/slack": "",
    },
    automation_condition=(
        AutomationCondition.cron_tick_passed("0 5 * * *")
        & AutomationCondition.all_deps_updated_since_cron("0 5 * * *")
        & ~AutomationCondition.in_progress()
    ),
    deps=["buildkite_builds", "buildkite_jobs"],
    description="Daily analysis of Buildkite builds using Claude, posted to Slack.",
)
def buildkite_daily_analysis(
    context: dg.AssetExecutionContext,
    config: BuildkiteAnalysisConfig,
    snowflake: SnowflakeResource,
    buildkite: BuildkiteResource,
    claude: AnthropicResource,
    slack_buildkite: SlackResource,
) -> dg.MaterializeResult:
    # Compute analysis time window
    window_end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=24)
    window_metadata = {
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }

    buildkite_sql = BuildkiteSQL(snowflake)

    builds = buildkite_sql.get_builds(
        pipelines=PIPELINES_TO_MONITOR,
        branch="master",
        window_start=window_start,
        window_end=window_end,
    )

    total, per_pipeline = summarize_builds(builds)
    context.log.info(
        "Build summary: %d total, %d passed, %d failed",
        total["total"],
        total["passed"],
        total["failed"],
    )

    if total["total"] == 0:
        context.log.info("No builds found in the last 24 hours, skipping analysis.")
        return dg.MaterializeResult(
            metadata={
                **window_metadata,
                "total_builds": 0,
                "status": "no_builds",
            }
        )

    # Fetch failed job logs
    failed_job_logs = extract_failed_job_logs(buildkite, builds)
    context.log.info("Fetched logs for %d failed jobs", len(failed_job_logs))

    # Build context for Claude
    claude_context = build_claude_context(total, per_pipeline, failed_job_logs, builds)

    # Call Claude for analysis
    with claude.get_client(context) as client:
        try:
            response = client.messages.create(
                model="claude-opus-4-6",
                system=BUILDKITE_ANALYSIS_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": claude_context}],
                max_tokens=4096,
                thinking={"type": "enabled", "budget_tokens": 2048},
            )
            analysis_text = response.content[-1].text
        except Exception as e:
            raise dg.Failure(f"Claude analysis failed: {e!s}") from e

    context.log.info("Claude analysis preview: %s", analysis_text[:500])

    # Post to Slack (prod only)
    if get_environment() != "PROD" and not config.force_slack_post:
        context.log.info(
            "Skipping Slack notification in non-prod environment (%s)",
            get_environment(),
        )
        return dg.MaterializeResult(
            metadata={
                **window_metadata,
                "total_builds": total["total"],
                "total_passed": total["passed"],
                "total_failed": total["failed"],
                "analysis_preview": analysis_text[:500],
                "slack_posted": False,
            }
        )

    blocks = format_slack_blocks(analysis_text)
    slack_client = slack_buildkite.get_client()
    slack_client.chat_postMessage(channel="#bot-buildkite-analysis", blocks=blocks)
    context.log.info("Posted analysis to #bot-buildkite-analysis")

    return dg.MaterializeResult(
        metadata={
            **window_metadata,
            "total_builds": total["total"],
            "total_passed": total["passed"],
            "total_failed": total["failed"],
            "analysis_preview": analysis_text[:500],
            "slack_posted": True,
        }
    )
