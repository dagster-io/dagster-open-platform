from datetime import datetime, timedelta, timezone

import dagster as dg
from dagster_anthropic import AnthropicResource
from dagster_slack import SlackResource

from dagster_open_platform.defs.buildkite.prompt import BUILDKITE_ANALYSIS_SYSTEM_PROMPT
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource
from dagster_open_platform.defs.buildkite.utils import (
    build_claude_context,
    extract_failed_job_logs,
    fetch_build_data,
    format_slack_blocks,
    summarize_builds,
)
from dagster_open_platform.utils.environment_helpers import get_environment

PIPELINES_TO_MONITOR = ["internal"]


class BuildkiteAnalysisConfig(dg.Config):
    force_slack_post: bool = False


@dg.asset(
    group_name="buildkite",
    tags={"dagster/kind/buildkite": "", "dagster/kind/anthropic": "", "dagster/kind/slack": ""},
    automation_condition=dg.AutomationCondition.cron_tick_passed("0 5 * * *")
    & ~dg.AutomationCondition.in_progress(),
    description="Daily analysis of Buildkite builds using Claude, posted to Slack.",
)
def buildkite_daily_analysis(
    context: dg.AssetExecutionContext,
    config: BuildkiteAnalysisConfig,
    buildkite: BuildkiteResource,
    claude: AnthropicResource,
    slack_buildkite: SlackResource,
) -> dg.MaterializeResult:
    # Compute analysis time window
    now = datetime.now(tz=timezone.utc)
    window_start = now - timedelta(hours=24)
    window_metadata = {
        "window_start": window_start.isoformat(),
        "window_end": now.isoformat(),
    }

    # Fetch builds from last 24 hours
    builds_by_pipeline = fetch_build_data(
        buildkite, PIPELINES_TO_MONITOR, lookback_hours=24, branch="master"
    )

    summary = summarize_builds(builds_by_pipeline)
    context.log.info(
        "Build summary: %d total, %d passed, %d failed",
        summary["total"],
        summary["passed"],
        summary["failed"],
    )

    if summary["total"] == 0:
        context.log.info("No builds found in the last 24 hours, skipping analysis.")
        return dg.MaterializeResult(
            metadata={
                **window_metadata,
                "total_builds": 0,
                "status": "no_builds",
            }
        )

    # Fetch failed job logs
    failed_job_logs = extract_failed_job_logs(buildkite, builds_by_pipeline)
    context.log.info("Fetched logs for %d failed jobs", len(failed_job_logs))

    # Build context for Claude
    claude_context = build_claude_context(summary, failed_job_logs, builds_by_pipeline)

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
            "Skipping Slack notification in non-prod environment (%s)", get_environment()
        )
        return dg.MaterializeResult(
            metadata={
                **window_metadata,
                "total_builds": summary["total"],
                "total_failed": summary["failed"],
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
            "total_builds": summary["total"],
            "total_failed": summary["failed"],
            "analysis_preview": analysis_text[:500],
            "slack_posted": True,
        }
    )
