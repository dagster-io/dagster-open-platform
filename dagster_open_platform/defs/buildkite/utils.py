import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from dagster_open_platform.defs.buildkite.resources import BuildkiteResource

logger = logging.getLogger(__name__)

MAX_FAILED_JOBS_TO_FETCH = 15
MAX_LOG_CHARS = 4000


def fetch_build_data(
    buildkite: BuildkiteResource,
    pipelines: list[str],
    lookback_hours: int = 24,
    branch: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch builds from the last N hours for the given pipelines.

    Returns:
        Dict mapping pipeline slug to list of build dicts.
    """
    created_from = (datetime.now(tz=timezone.utc) - timedelta(hours=lookback_hours)).isoformat()
    builds_by_pipeline: dict[str, list[dict[str, Any]]] = {}
    for pipeline in pipelines:
        builds = buildkite.get_builds(
            pipeline_slug=pipeline, created_from=created_from, branch=branch
        )
        builds_by_pipeline[pipeline] = builds
    return builds_by_pipeline


def extract_failed_job_logs(
    buildkite: BuildkiteResource,
    builds_by_pipeline: dict[str, list[dict[str, Any]]],
) -> list[dict[str, str]]:
    """Fetch logs for failed jobs, capped at MAX_FAILED_JOBS_TO_FETCH jobs.

    Returns:
        List of dicts with keys: pipeline, build_number, job_name, job_id, log_tail.
    """
    failed_jobs: list[dict[str, str]] = []

    for pipeline, builds in builds_by_pipeline.items():
        for build in builds:
            build_number = build.get("number")
            for job in build.get("jobs", []):
                if job.get("state") == "failed" and job.get("type") == "script":
                    failed_jobs.append(
                        {
                            "pipeline": pipeline,
                            "build_number": str(build_number),
                            "job_name": job.get("name", "unknown"),
                            "job_id": job.get("id", ""),
                        }
                    )
                    if len(failed_jobs) >= MAX_FAILED_JOBS_TO_FETCH:
                        break
            if len(failed_jobs) >= MAX_FAILED_JOBS_TO_FETCH:
                break
        if len(failed_jobs) >= MAX_FAILED_JOBS_TO_FETCH:
            break

    results: list[dict[str, str]] = []
    for job_info in failed_jobs:
        try:
            log_text = buildkite.get_job_log(
                pipeline_slug=job_info["pipeline"],
                build_number=int(job_info["build_number"]),
                job_id=job_info["job_id"],
            )
            # Take the tail of the log to capture the failure output
            log_tail = log_text[-MAX_LOG_CHARS:] if log_text else "(no log output)"
        except Exception:
            logger.warning(
                "Failed to fetch log for job %s in build %s",
                job_info["job_id"],
                job_info["build_number"],
                exc_info=True,
            )
            log_tail = "(log unavailable)"

        results.append({**job_info, "log_tail": log_tail})

    return results


def summarize_builds(
    builds_by_pipeline: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Compute pass/fail stats for builds.

    Returns:
        Dict with total, passed, failed counts and per-pipeline breakdown.
    """
    total = 0
    passed = 0
    failed = 0
    per_pipeline: dict[str, dict[str, int]] = {}

    for pipeline, builds in builds_by_pipeline.items():
        p_total = len(builds)
        p_passed = sum(1 for b in builds if b.get("state") == "passed")
        p_failed = sum(1 for b in builds if b.get("state") == "failed")
        per_pipeline[pipeline] = {
            "total": p_total,
            "passed": p_passed,
            "failed": p_failed,
        }
        total += p_total
        passed += p_passed
        failed += p_failed

    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "per_pipeline": per_pipeline,
    }


def build_claude_context(
    summary: dict[str, Any],
    failed_job_logs: list[dict[str, str]],
    builds_by_pipeline: dict[str, list[dict[str, Any]]],
) -> str:
    """Assemble the markdown context string for Claude.

    Includes build stats, per-job pass/fail counts, and failed job log tails.
    """
    lines: list[str] = []
    lines.append("# Buildkite Build Analysis - Last 24 Hours\n")

    # Overall stats
    lines.append(f"**Total builds**: {summary['total']}")
    lines.append(f"**Passed**: {summary['passed']}")
    lines.append(f"**Failed**: {summary['failed']}\n")

    # Per-pipeline stats
    for pipeline, stats in summary["per_pipeline"].items():
        lines.append(f"## Pipeline: {pipeline}")
        lines.append(
            f"  Builds: {stats['total']} | Passed: {stats['passed']} | Failed: {stats['failed']}"
        )

    # Job-level pass/fail for flakiness detection
    job_results: dict[str, dict[str, int]] = {}
    for builds in builds_by_pipeline.values():
        for build in builds:
            for job in build.get("jobs", []):
                if job.get("type") != "script":
                    continue
                name = job.get("name", "unknown")
                if name not in job_results:
                    job_results[name] = {"passed": 0, "failed": 0}
                state = job.get("state", "")
                if state == "passed":
                    job_results[name]["passed"] += 1
                elif state == "failed":
                    job_results[name]["failed"] += 1

    lines.append("\n## Job-Level Results")
    for name, counts in sorted(job_results.items()):
        lines.append(f"- **{name}**: {counts['passed']} passed, {counts['failed']} failed")

    # Failed job logs
    if failed_job_logs:
        lines.append("\n## Failed Job Logs (tail)\n")
        for entry in failed_job_logs:
            lines.append(f"### {entry['job_name']} (build #{entry['build_number']})")
            lines.append(f"```\n{entry['log_tail']}\n```\n")

    return "\n".join(lines)


def format_slack_blocks(response_text: str) -> list[dict[str, Any]]:
    """Parse Claude's response into Slack Block Kit JSON.

    Attempts to extract a JSON array from the response. Falls back to a plain
    text section block if parsing fails.
    """
    # Try to find a JSON array in the response
    text = response_text.strip()

    # If the response starts with [ it's likely the JSON directly
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        json_str = text[start : end + 1]
        try:
            blocks = json.loads(json_str)
            if isinstance(blocks, list):
                return blocks
        except json.JSONDecodeError:
            pass

    # Fallback: wrap the raw text in a section block
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": response_text[:3000],
            },
        }
    ]
