import json
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from typing import Any, ClassVar

from dagster_snowflake import SnowflakeResource
from snowflake.connector.cursor import SnowflakeCursor

from dagster_open_platform.defs.buildkite.models import Build, Job
from dagster_open_platform.utils.environment_helpers import get_environment


class BuildkiteSQL:
    __snowflake: SnowflakeResource
    __database: str

    _BUILDS_TABLE_NAME: ClassVar[str] = "buildkite_builds"
    _JOBS_TABLE_NAME: ClassVar[str] = "buildkite_jobs"
    _BUILDS_ID_COLUMN_NAME: ClassVar[str] = "build_id"
    _JOBS_ID_COLUMN_NAME: ClassVar[str] = "job_id"

    # Structural columns. ai_assessment is handled separately on the write
    # path because it's a VARIANT (round-trips through PARSE_JSON / TO_JSON)
    # and gets clobber-protection on UPDATE so out-of-band backfills survive
    # re-materialization of the same partition.
    __BUILDS_COLS: tuple[str, ...] = (
        "build_id", "extracted_at", "pipeline__id", "pipeline__slug", "pipeline__name",
        "url", "web_url", "build_number", "state", "blocked", "cancel_reason", "message",
        "commit", "branch", "source", "created_at", "scheduled_at", "started_at", "finished_at",
    )  # fmt: skip
    __JOBS_COLS: tuple[str, ...] = (
       "job_id", "build_id", "extracted_at", "type", "name", "step_key", "group_key",
       "state", "command", "soft_failed", "exit_status", "retried", "retries_count",
       "created_at", "scheduled_at", "runnable_at", "started_at", "finished_at", "expired_at"
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

    def __ensure_builds_schema(self, cursor: SnowflakeCursor) -> None:
        """Idempotent CREATE TABLE + ALTER ADD COLUMN for the builds table.

        Called from both write and read paths so that a read can't 404 on new
        columns during the post-deploy window before the first write run
        has migrated the existing prod table.
        """
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._BUILDS_TABLE_NAME} (
                {self._BUILDS_ID_COLUMN_NAME} VARCHAR(100) PRIMARY KEY,
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
        """)
        cursor.execute(
            f"ALTER TABLE {self._BUILDS_TABLE_NAME} ADD COLUMN IF NOT EXISTS ai_assessment VARIANT"
        )

    def __ensure_jobs_schema(self, cursor: SnowflakeCursor) -> None:
        """Idempotent CREATE TABLE + ALTER ADD COLUMN for the jobs table.

        Called from both write and read paths so that a read can't 404 on new
        columns during the post-deploy window before the first write run
        has migrated the existing prod table.
        """
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._JOBS_TABLE_NAME} (
                {self._JOBS_ID_COLUMN_NAME} VARCHAR(100) PRIMARY KEY,
                {self._BUILDS_ID_COLUMN_NAME} VARCHAR(100) REFERENCES buildkite_builds({self._BUILDS_ID_COLUMN_NAME}),
                extracted_at TIMESTAMP_TZ,
                type VARCHAR(100),
                name VARCHAR(1000),
                step_key VARCHAR(500),
                group_key VARCHAR(500),
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
        """)
        cursor.execute(
            f"ALTER TABLE {self._JOBS_TABLE_NAME} ADD COLUMN IF NOT EXISTS group_key VARCHAR(500)"
        )

    def __upsert_builds(self, cursor: SnowflakeCursor, rows: list[tuple[Any]]) -> None:
        """Stage rows in a temp table, then MERGE into the target.

        Each row is `(*structural_values, ai_assessment_json_str)` — see
        ``__build_to_insert_row``. New builds are INSERTed with all columns
        including ai_assessment. Existing builds have only ai_assessment
        UPDATEd; structural columns are immutable for finished builds (the
        ingestion filters ``state="finished"``). The UPDATE has clobber-
        protection: if the inbound payload is empty (``{}`` — e.g. a build
        on a branch outside the assessment gate), the existing target value
        wins, so out-of-band backfills survive re-materialization.
        """
        TMP_BUILDS_TABLE_NAME = "tmp_builds"
        all_cols = (*self.__BUILDS_COLS, "ai_assessment")
        n_cols = len(all_cols)

        all_cols_csv = ", ".join(all_cols)
        struct_csv = ", ".join(self.__BUILDS_COLS)
        placeholder_row = "(" + ", ".join(["%s"] * n_cols) + ")"
        values_clause = ", ".join(placeholder_row for _ in rows)
        flat_params = [val for row in rows for val in row]
        insert_vals = ", ".join(f"src.{c}" for c in all_cols)

        # PARSE_JSON can't appear inside a parameterized VALUES list (the
        # connector's bulk-binding rewrite strips it), so the staging INSERT
        # wraps VALUES in a SELECT projection that calls PARSE_JSON on the
        # ai_assessment column.
        cursor.execute(
            f"CREATE TEMPORARY TABLE {TMP_BUILDS_TABLE_NAME} AS "
            f"SELECT * FROM {self._BUILDS_TABLE_NAME} WHERE FALSE"
        )
        cursor.execute(
            f"INSERT INTO {TMP_BUILDS_TABLE_NAME} ({all_cols_csv}) "
            f"SELECT {struct_csv}, PARSE_JSON(ai_assessment) "
            f"FROM (VALUES {values_clause}) AS t({struct_csv}, ai_assessment)",
            flat_params,
        )
        cursor.execute(f"""
            MERGE INTO {self._BUILDS_TABLE_NAME} tgt
            USING {TMP_BUILDS_TABLE_NAME} src ON tgt.{self._BUILDS_ID_COLUMN_NAME} = src.{self._BUILDS_ID_COLUMN_NAME}
            WHEN MATCHED THEN UPDATE SET ai_assessment = COALESCE(
                NULLIF(src.ai_assessment, PARSE_JSON('{{}}')),
                tgt.ai_assessment
            )
            WHEN NOT MATCHED THEN INSERT ({all_cols_csv}) VALUES ({insert_vals})
        """)
        cursor.execute(f"DROP TABLE IF EXISTS {TMP_BUILDS_TABLE_NAME}")

    def __upsert_jobs(self, cursor: SnowflakeCursor, rows: list[tuple[Any]]) -> None:
        TMP_JOBS_TABLE_NAME = "tmp_jobs"

        cols = ", ".join(self.__JOBS_COLS)
        insert_vals = ", ".join(f"src.{c}" for c in self.__JOBS_COLS)

        cursor.execute(
            f"CREATE TEMPORARY TABLE {TMP_JOBS_TABLE_NAME} AS SELECT * FROM {self._JOBS_TABLE_NAME} WHERE FALSE"
        )

        # snowflake's connector has a hard limit on the number of expressions in a list (200_000 at time of writing)
        # the partitions with the largest number of jobs can exceed this, so we have to chunk the inserts
        chunk_size = 5_000
        for i in range(0, len(rows), chunk_size):
            row_chunk = rows[i : i + chunk_size]
            values_clause = ",\n".join(
                f"({', '.join(['%s'] * len(self.__JOBS_COLS))})" for _ in row_chunk
            )
            flat_params = [val for row in row_chunk for val in row]

            cursor.execute(
                f"INSERT INTO {TMP_JOBS_TABLE_NAME} ({cols}) VALUES {values_clause}", flat_params
            )

        cursor.execute(f"""
            MERGE INTO {self._JOBS_TABLE_NAME} tgt
            USING {TMP_JOBS_TABLE_NAME} src ON tgt.{self._JOBS_ID_COLUMN_NAME} = src.{self._JOBS_ID_COLUMN_NAME}
            WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({insert_vals})
        """)
        cursor.execute(f"DROP TABLE IF EXISTS {TMP_JOBS_TABLE_NAME}")

    def insert_builds(self, builds: list[Build]) -> None:
        if not builds:
            return

        rows = [BuildkiteSQL.__build_to_insert_row(b) for b in builds]
        with self.__get_cursor() as cursor:
            self.__ensure_builds_schema(cursor)
            self.__upsert_builds(cursor, rows)

    def insert_jobs(self, jobs: list[Job]) -> None:
        if not jobs:
            return

        rows = [BuildkiteSQL.__job_to_insert_row(j) for j in jobs]
        with self.__get_cursor() as cursor:
            self.__ensure_jobs_schema(cursor)
            self.__upsert_jobs(cursor, rows)

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
                SELECT {", ".join(self.__BUILDS_COLS)}, TO_JSON(ai_assessment) AS ai_assessment
                FROM {self._BUILDS_TABLE_NAME}
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
                SELECT {", ".join(self.__JOBS_COLS)}
                FROM {self._JOBS_TABLE_NAME}
                WHERE {self._BUILDS_ID_COLUMN_NAME} IN ({", ".join(["%s"] * len(build_ids))})
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
        # Order: __BUILDS_COLS, then ai_assessment as a JSON string (PARSE_JSON
        # is applied SQL-side; the connector can't bind a Python dict to a
        # VARIANT column directly).
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
        # Order must match __JOBS_COLS
        return (
            job.id,
            job.build_id,
            job.extracted_at,
            job.type,
            job.name,
            job.step_key,
            job.group_key,
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
            id=row[BuildkiteSQL._BUILDS_ID_COLUMN_NAME],
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
            id=row[BuildkiteSQL._JOBS_ID_COLUMN_NAME],
            build_id=row[BuildkiteSQL._BUILDS_ID_COLUMN_NAME],
            extracted_at=row["extracted_at"],
            type=row["type"],
            name=row.get("name"),
            step_key=row.get("step_key"),
            group_key=row.get("group_key"),
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
