import json
import logging
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from dagster_snowflake import SnowflakeResource
from snowflake.connector.cursor import SnowflakeCursor

from dagster_open_platform.defs.buildkite.models import Build, Job
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource
from dagster_open_platform.utils.environment_helpers import get_environment

logger = logging.getLogger(__name__)

MAX_FAILED_JOBS_TO_FETCH = 15
MAX_LOG_CHARS = 4000


class BuildkiteSQL:
    __snowflake: SnowflakeResource
    __database: str

    __BUILDS_TABLE_NAME: str = "buildkite_builds"
    __JOBS_TABLE_NAME: str = "buildkite_jobs"
    __JOBS_ID_COLUMN_NAME: str = "job_id"

    # Single source of truth for the buildkite_builds column order. Drives the
    # MERGE source SELECT, UPDATE SET, INSERT lists, the read-path SELECT, and
    # the row tuple produced by __build_to_insert_row. ai_assessment is
    # handled separately because it round-trips through PARSE_JSON / TO_JSON
    # and has clobber-protection on UPDATE.
    __BUILD_COLS: tuple[str, ...] = (
        "build_id", "extracted_at", "pipeline__id", "pipeline__slug", "pipeline__name",
        "url", "web_url", "build_number", "state", "blocked", "cancel_reason", "message",
        "commit", "branch", "source", "created_at", "scheduled_at", "started_at", "finished_at",
    )  # fmt: skip

    def __init__(self, snowflake: SnowflakeResource):
        self.__database = "BUILDKITE" if get_environment() == "PROD" else "SANDBOX"
        self.__snowflake = snowflake

    @contextmanager
    def __get_cursor(self) -> Generator[SnowflakeCursor, None, None]:
        with self.__snowflake.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(f"USE DATABASE {self.__database}")

            try:
                cursor.execute("BEGIN")
                yield cursor
                cursor.execute("COMMIT")
            except Exception:
                cursor.execute("ROLLBACK")
                raise
            finally:
                cursor.close()

    def __create_builds_table(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.__BUILDS_TABLE_NAME} (
                build_id VARCHAR(100) PRIMARY KEY,
                extracted_at TIMESTAMP_TZ,
                pipeline__id VARCHAR(100),
                pipeline__slug VARCHAR(200),
                pipeline__name VARCHAR(200),
                url VARCHAR(500),
                web_url VARCHAR(500),
                build_number NUMBER,
                state VARCHAR(50),
                blocked BOOLEAN,
                cancel_reason VARCHAR(1000),
                message VARCHAR(65536),
                commit VARCHAR(100),
                branch VARCHAR(500),
                source VARCHAR(100),
                created_at TIMESTAMP_TZ,
                scheduled_at TIMESTAMP_TZ,
                started_at TIMESTAMP_TZ,
                finished_at TIMESTAMP_TZ,
                ai_assessment VARIANT
            )
        """

    def __create_jobs_table(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.__JOBS_TABLE_NAME} (
                job_id VARCHAR(100) PRIMARY KEY,
                build_id VARCHAR(100) REFERENCES buildkite_builds(build_id),
                extracted_at TIMESTAMP_TZ,
                type VARCHAR(100),
                name VARCHAR(1000),
                step_key VARCHAR(500),
                state VARCHAR(50),
                command VARCHAR(65536),
                soft_failed BOOLEAN,
                exit_status NUMBER,
                retried BOOLEAN,
                retries_count NUMBER,
                created_at TIMESTAMP_TZ,
                scheduled_at TIMESTAMP_TZ,
                runnable_at TIMESTAMP_TZ,
                started_at TIMESTAMP_TZ,
                finished_at TIMESTAMP_TZ,
                expired_at TIMESTAMP_TZ
            )
        """

    def __delete_jobs_table_old_rows(self, ids: list[str]) -> tuple[str, list[str]]:
        return (
            f"""
                DELETE FROM {self.__JOBS_TABLE_NAME} WHERE {self.__JOBS_ID_COLUMN_NAME} IN ({",".join(["%s"] * len(ids))})
            """,
            ids,
        )

    def __merge_builds_table_rows(self, rows: list[tuple[Any]]) -> tuple[str, list[tuple[Any]]]:
        # MERGE (not DELETE+INSERT) so an out-of-band ai_assessment write — e.g.
        # an ad-hoc backfill that UPDATEs the column directly — is preserved
        # when the live ingestion later re-materializes the same partition with
        # an empty ai_assessment. COALESCE(NULLIF(src, '{}'), tgt) keeps tgt
        # only when the inbound payload is the empty object.
        src_select = ", ".join(f"%s AS {c}" for c in self.__BUILD_COLS)
        update_set = ",\n                    ".join(
            f"{c} = src.{c}" for c in self.__BUILD_COLS if c != "build_id"
        )
        insert_cols = ", ".join((*self.__BUILD_COLS, "ai_assessment"))
        insert_vals = ", ".join(f"src.{c}" for c in (*self.__BUILD_COLS, "ai_assessment"))
        return (
            f"""
                MERGE INTO {self.__BUILDS_TABLE_NAME} tgt
                USING (SELECT {src_select}, PARSE_JSON(%s) AS ai_assessment) src
                ON tgt.build_id = src.build_id
                WHEN MATCHED THEN UPDATE SET
                    {update_set},
                    ai_assessment = COALESCE(
                        NULLIF(src.ai_assessment, PARSE_JSON('{{}}')),
                        tgt.ai_assessment
                    )
                WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
            """,
            rows,
        )

    def __insert_jobs_table_new_rows(self, rows: list[tuple[Any]]) -> tuple[str, list[tuple[Any]]]:
        return (
            f"""
                INSERT INTO {self.__JOBS_TABLE_NAME} (
                    job_id, build_id, extracted_at, type, name, step_key, state, command, soft_failed, exit_status,
                    retried, retries_count, created_at, scheduled_at, runnable_at, started_at, finished_at, expired_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )

    def __ensure_builds_schema(self, cursor: SnowflakeCursor) -> None:
        """Idempotent CREATE TABLE + ALTER ADD COLUMN for the builds table.

        Called from both write and read paths so that a read can't 404 on
        `ai_assessment` during the post-deploy window before the first writer
        run has migrated the existing prod table. Both statements are no-ops
        once satisfied.
        """
        cursor.execute(self.__create_builds_table())
        cursor.execute(
            f"ALTER TABLE {self.__BUILDS_TABLE_NAME} ADD COLUMN IF NOT EXISTS ai_assessment VARIANT"
        )

    def insert_builds(self, builds: list[Build]) -> None:
        if not builds:
            return

        rows = [BuildkiteSQL.__build_to_insert_row(b) for b in builds]
        with self.__get_cursor() as cursor:
            self.__ensure_builds_schema(cursor)
            cursor.executemany(*self.__merge_builds_table_rows(rows))

    def insert_jobs(self, jobs: list[Job]) -> None:
        if not jobs:
            return

        rows = [BuildkiteSQL.__job_to_insert_row(j) for j in jobs]
        with self.__get_cursor() as cursor:
            cursor.execute(self.__create_jobs_table())
            cursor.execute(*self.__delete_jobs_table_old_rows([j.id for j in jobs]))
            cursor.executemany(*self.__insert_jobs_table_new_rows(rows))

    def get_builds(
        self,
        *,
        pipelines: list[str] | None = None,
        branch: str | None = None,
        window_start: datetime | None = None,
        window_end: datetime | None = None,
    ) -> list[Build]:
        conditions: list[str] = []
        build_params: list[Any] = []

        if pipelines:
            conditions.append(f"pipeline__slug IN ({','.join(['%s'] * len(pipelines))})")
            build_params.extend(pipelines)
        if branch:
            conditions.append("branch = %s")
            build_params.append(branch)
        if window_start:
            conditions.append("created_at >= %s")
            build_params.append(window_start.isoformat())
        if window_end:
            conditions.append("created_at < %s")
            build_params.append(window_end.isoformat())

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        with self.__get_cursor() as cursor:
            self.__ensure_builds_schema(cursor)
            cursor.execute(
                f"""
                SELECT {", ".join(self.__BUILD_COLS)}, TO_JSON(ai_assessment) AS ai_assessment
                FROM buildkite_builds
                {where_clause}
                """,
                build_params,
            )
            build_columns = [col[0].lower() for col in cursor.description]
            build_rows = [dict(zip(build_columns, row)) for row in cursor.fetchall()]
            if not build_rows:
                return []

            build_ids = [r["build_id"] for r in build_rows]
            cursor.execute(
                f"""
                SELECT job_id, build_id, extracted_at, type, name, step_key, state, command, soft_failed, exit_status,
                    retried, retries_count, created_at, scheduled_at, runnable_at, started_at, finished_at, expired_at
                FROM buildkite_jobs
                WHERE build_id IN ({",".join(["%s"] * len(build_ids))})
                """,
                build_ids,
            )
            job_columns = [col[0].lower() for col in cursor.description]
            job_rows = [dict(zip(job_columns, row)) for row in cursor.fetchall()]

            jobs_by_build: dict[str, list[Job]] = {id: [] for id in build_ids}
            for row in job_rows:
                job = BuildkiteSQL.__job_from_row(row)
                jobs_by_build[job.build_id].append(job)

            return [
                BuildkiteSQL.__build_from_row(r, jobs_by_build[r["build_id"]]) for r in build_rows
            ]

    @staticmethod
    def __build_to_insert_row(build: Build) -> tuple:
        # Order must match __BUILD_COLS, with the JSON-encoded ai_assessment
        # appended (consumed by PARSE_JSON in the MERGE).
        return (
            build.id,
            build.extracted_at,
            build.pipeline__id,
            build.pipeline__slug,
            build.pipeline__name,
            build.url,
            build.web_url,
            build.number,
            build.state,
            build.blocked,
            build.cancel_reason,
            build.message,
            build.commit,
            build.branch,
            build.source,
            build.created_at,
            build.scheduled_at,
            build.started_at,
            build.finished_at,
            json.dumps(build.ai_assessment),
        )

    @staticmethod
    def __job_to_insert_row(job: Job) -> tuple:
        # Order must match INSERT INTO buildkite_jobs column list
        return (
            job.id,
            job.build_id,
            job.extracted_at,
            job.type,
            job.name,
            job.step_key,
            job.state,
            job.command,
            job.soft_failed,
            job.exit_status,
            job.retried,
            job.retries_count,
            job.created_at,
            job.scheduled_at,
            job.runnable_at,
            job.started_at,
            job.finished_at,
            job.expired_at,
        )

    @staticmethod
    def __build_from_row(row: dict[str, Any], jobs: list[Job]) -> Build:
        ai_assessment_raw = row.get("ai_assessment")
        ai_assessment = json.loads(ai_assessment_raw) if ai_assessment_raw else {}
        return Build(
            id=row["build_id"],
            extracted_at=row["extracted_at"],
            pipeline__id=row["pipeline__id"],
            pipeline__slug=row["pipeline__slug"],
            pipeline__name=row["pipeline__name"],
            url=row["url"],
            web_url=row["web_url"],
            number=row["build_number"],
            state=row["state"],
            blocked=row["blocked"],
            cancel_reason=row.get("cancel_reason"),
            message=row["message"],
            commit=row["commit"],
            branch=row["branch"],
            source=row["source"],
            created_at=row.get("created_at"),
            scheduled_at=row.get("scheduled_at"),
            started_at=row.get("started_at"),
            finished_at=row.get("finished_at"),
            ai_assessment=ai_assessment,
            jobs=jobs,
        )

    @staticmethod
    def __job_from_row(row: dict[str, Any]) -> Job:
        return Job(
            id=row["job_id"],
            build_id=row["build_id"],
            extracted_at=row["extracted_at"],
            type=row["type"],
            name=row.get("name"),
            step_key=row.get("step_key"),
            state=row.get("state"),
            command=row.get("command"),
            soft_failed=row.get("soft_failed"),
            exit_status=row.get("exit_status"),
            retried=row.get("retried"),
            retries_count=row.get("retries_count"),
            created_at=row.get("created_at"),
            scheduled_at=row.get("scheduled_at"),
            runnable_at=row.get("runnable_at"),
            started_at=row.get("started_at"),
            finished_at=row.get("finished_at"),
            expired_at=row.get("expired_at"),
        )


def extract_failed_job_logs(
    buildkite: BuildkiteResource,
    builds: list[Build],
) -> list[dict[str, str]]:
    """Fetch logs for failed jobs, capped at MAX_FAILED_JOBS_TO_FETCH jobs.

    Returns:
        List of dicts with keys: pipeline, build_number, job_name, job_id, log_tail.
    """
    failed_jobs: list[dict[str, str]] = []

    for build in builds:
        for job in build.jobs:
            if job.state == "failed" and job.type == "script":
                failed_jobs.append(
                    {
                        "pipeline": build.pipeline__slug,
                        "build_number": str(build.number),
                        "job_name": job.name or "unknown",
                        "job_id": job.id,
                    }
                )
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
    builds: list[Build],
) -> tuple[
    dict[str, int],
    dict[str, dict[str, int]],
]:
    """Compute pass/fail stats for builds.

    Returns:
        Overall totals and a per-pipeline breakdown.
    """
    total: dict[str, int] = {"total": 0, "passed": 0, "failed": 0}
    per_pipeline: dict[str, dict[str, int]] = {}

    for build in builds:
        slug = build.pipeline__slug
        if slug not in per_pipeline:
            per_pipeline[slug] = {"total": 0, "passed": 0, "failed": 0}

        total["total"] += 1
        per_pipeline[slug]["total"] += 1
        if build.state == "passed":
            total["passed"] += 1
            per_pipeline[slug]["passed"] += 1
        elif build.state == "failed":
            total["failed"] += 1
            per_pipeline[slug]["failed"] += 1

    return total, per_pipeline


def build_claude_context(
    total: dict[str, int],
    per_pipeline: dict[str, dict[str, int]],
    failed_job_logs: list[dict[str, str]],
    builds: list[Build],
) -> str:
    """Assemble the markdown context string for Claude.

    Includes build stats, per-job pass/fail counts, and failed job log tails.
    """
    lines: list[str] = []
    lines.append("# Buildkite Build Analysis - Last 24 Hours\n")

    lines.append(f"**Total builds**: {total['total']}")
    lines.append(f"**Passed**: {total['passed']}")
    lines.append(f"**Failed**: {total['failed']}\n")

    for pipeline, stats in per_pipeline.items():
        lines.append(f"## Pipeline: {pipeline}")
        lines.append(
            f"  Builds: {stats['total']} | Passed: {stats['passed']} | Failed: {stats['failed']}"
        )

    # Job-level pass/fail for flakiness detection
    job_pass_count: dict[str, int] = {}
    job_fail_count: dict[str, int] = {}
    job_failed_build_urls: dict[str, list[str]] = {}
    job_passed_build_urls: dict[str, list[str]] = {}
    for build in builds:
        for job in build.jobs:
            if job.type != "script":
                continue
            name = job.name or "unknown"
            if name not in job_pass_count:
                job_pass_count[name] = 0
                job_fail_count[name] = 0
                job_failed_build_urls[name] = []
                job_passed_build_urls[name] = []
            if job.state == "passed":
                job_pass_count[name] += 1
                job_passed_build_urls[name].append(build.web_url)
            elif job.state == "failed":
                job_fail_count[name] += 1
                job_failed_build_urls[name].append(build.web_url)

    lines.append("\n## Job-Level Results")
    for name in sorted(job_pass_count):
        passed = job_pass_count[name]
        failed = job_fail_count[name]
        line = f"- **{name}**: {passed} passed, {failed} failed"
        if failed and passed:
            # Flaky: include representative build links for both outcomes
            line += f"\n  - Example failed build: {job_failed_build_urls[name][0]}"
            line += f"\n  - Example passed build: {job_passed_build_urls[name][0]}"
        elif failed:
            line += f"\n  - Example failed build: {job_failed_build_urls[name][0]}"
        lines.append(line)

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
