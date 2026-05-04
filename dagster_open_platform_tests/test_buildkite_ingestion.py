"""Tests for the Buildkite ingestion `ai_assessment` plumbing.

Buildkite's `meta_data` API returns a string→string map; the post-build AI
assessment step writes its structured payload as JSON under the single
`ai_assessment` key. We extract and parse just that key into
`Build.ai_assessment` (a `dict[str, Any]`), discarding the rest of the BK
meta-data (`build-start-time`, `buildkite:git:commit`, etc.).

Tests here exercise the API → `Build` → row → `Build` round-trip via mocked
HTTP and Snowflake cursor.
"""

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

from dagster_open_platform.defs.buildkite.models import Build
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource
from dagster_open_platform.defs.buildkite.utils import BuildkiteSQL

# ########################
# ##### FIXTURES
# ########################

_EXTRACTED_AT = datetime(2026, 4, 29, 12, 0, 0, tzinfo=timezone.utc)

_ASSESSMENT_PAYLOAD: dict[str, Any] = {
    "commit_status": "clean",
    "failure_level": "flake",
    "total_failed_jobs": 1,
    "analyzed_failed_jobs": 1,
    "summary": "All failures look like flakes.",
    "genuine_job_failures": [],
    "false_job_failures": [
        {
            "step_key": "dagster-pytest",
            "display_key": "dagster-pytest",
            "job_id": "job-uuid-1",
            "queue": "kubernetes-gke",
            "category": "infra",
            "reason": "Network timeout to Postgres fixture container",
            "test_names": [],
            "dropped_test_names_count": 0,
        },
    ],
    "details": "All flagged failures match prior known flakes.",
    "input_tokens": 1234,
    "output_tokens": 456,
    "model": "anthropic.claude-opus-4-7",
}

# BK meta-data values are strings, so the assessment lands under `ai_assessment`
# as a JSON-encoded blob alongside the rendered annotation markdown and
# unrelated BK-native keys (`build-start-time`, `buildkite:git:commit`). The
# ingestion picks out just the assessment.
_API_META_DATA: dict[str, str] = {
    "ai_assessment": json.dumps(_ASSESSMENT_PAYLOAD),
    "ai_assessment_annotation_md": (
        "## :robot_face: Assessment: clean (all job failures are flakes)\n"
        "All failures look like flakes."
    ),
    "build-start-time": "1777736299\n",
    "buildkite:git:commit": "commit abc1234\nAuthor: ...",
}


def _api_build(meta_data: Any = None) -> dict[str, Any]:
    """Buildkite REST-shape build dict; meta_data field is freely overridable."""
    payload: dict[str, Any] = {
        "id": "build-uuid-1",
        "pipeline": {"id": "pipe-1", "slug": "internal", "name": "internal"},
        "url": "https://api.buildkite.com/v2/x",
        "web_url": "https://buildkite.com/dagster/internal/builds/123",
        "number": 123,
        "state": "failed",
        "blocked": False,
        "cancel_reason": None,
        "message": "feat: do a thing",
        "commit": "abc1234",
        "branch": "feature-x",
        "source": "webhook",
        "created_at": "2026-04-29T11:55:00Z",
        "scheduled_at": "2026-04-29T11:55:01Z",
        "started_at": "2026-04-29T11:55:02Z",
        "finished_at": "2026-04-29T11:59:00Z",
        "jobs": [],
    }
    if meta_data is not _SENTINEL:
        payload["meta_data"] = meta_data
    return payload


_SENTINEL = object()


# ########################
# ##### TESTS
# ########################


def test_get_builds_extracts_ai_assessment_from_api_meta_data():
    """`BuildkiteResource.get_builds` parses the BK `meta_data.ai_assessment`
    JSON blob into `Build.ai_assessment`, returns {} when absent / null /
    malformed, ignores the other BK meta-data keys, and stops paginating on
    an empty page.
    """
    resource = BuildkiteResource(api_token="t", org_slug="dagster")

    test_cases = [
        # Full BK payload — only the parsed ai_assessment object survives.
        (_API_META_DATA, _ASSESSMENT_PAYLOAD),
        # API meta_data field absent → {}.
        (_SENTINEL, {}),
        # API meta_data field explicitly None → {}.
        (None, {}),
        # ai_assessment key absent (e.g. branch outside the assessment gate) → {}.
        ({"build-start-time": "1777736299\n"}, {}),
        # Malformed JSON → {} (logged, not raised).
        ({"ai_assessment": "not-json"}, {}),
    ]

    for api_value, expected in test_cases:
        api_payload = _api_build(api_value)
        with patch("dagster_open_platform.defs.buildkite.resources.requests.get") as mock_get:
            mock_response = MagicMock()
            mock_response.json.side_effect = [[api_payload], []]
            mock_get.return_value = mock_response
            builds = resource.get_builds()

        assert len(builds) == 1
        assert builds[0].ai_assessment == expected


def test_insert_builds():
    """`insert_builds` stages rows in a temp table then MERGEs insert-only
    into the target. ai_assessment is NOT written by this path (populated
    out-of-band). Verifies SQL shape and SELECT round-trip.
    """
    build = Build(
        id="b1",
        extracted_at=_EXTRACTED_AT,
        pipeline__id="p1",
        pipeline__slug="internal",
        pipeline__name="internal",
        url="https://api/x",
        web_url="https://buildkite/x",
        number=42,
        state="failed",
        blocked=False,
        cancel_reason=None,
        message="msg",
        commit="abc",
        branch="main",
        source="webhook",
        created_at=_EXTRACTED_AT,
        scheduled_at=_EXTRACTED_AT,
        started_at=_EXTRACTED_AT,
        finished_at=_EXTRACTED_AT,
        ai_assessment={},
    )

    cursor = MagicMock()
    snowflake = MagicMock()
    snowflake.get_connection.return_value.__enter__.return_value.cursor.return_value = cursor

    sql = BuildkiteSQL(snowflake)
    sql.insert_builds([build])

    execute_sqls = [c.args[0] for c in cursor.execute.call_args_list]

    # MERGE is insert-only
    merge_sql = next(s for s in execute_sqls if "MERGE INTO" in s and "buildkite_builds" in s)
    assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
    assert "WHEN MATCHED" not in merge_sql

    # SELECT round-trip: ai_assessment is read back via TO_JSON.
    cursor.description = [
        ("BUILD_ID",),
        ("EXTRACTED_AT",),
        ("PIPELINE__ID",),
        ("PIPELINE__SLUG",),
        ("PIPELINE__NAME",),
        ("URL",),
        ("WEB_URL",),
        ("BUILD_NUMBER",),
        ("STATE",),
        ("BLOCKED",),
        ("CANCEL_REASON",),
        ("MESSAGE",),
        ("COMMIT",),
        ("BRANCH",),
        ("SOURCE",),
        ("CREATED_AT",),
        ("SCHEDULED_AT",),
        ("STARTED_AT",),
        ("FINISHED_AT",),
        ("AI_ASSESSMENT",),
    ]
    ai_json = json.dumps(_ASSESSMENT_PAYLOAD)
    cursor.fetchall.side_effect = [
        [
            (
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
                ai_json,
            )
        ],
        [],  # no jobs
    ]
    rebuilt = sql.get_builds()
    assert len(rebuilt) == 1
    assert rebuilt[0].ai_assessment == _ASSESSMENT_PAYLOAD

    select_sql = next(
        s
        for s in [c.args[0] for c in cursor.execute.call_args_list]
        if "TO_JSON(ai_assessment)" in s
    )
    assert "FROM buildkite_builds" in select_sql


def test_get_builds_handles_null_ai_assessment_in_legacy_rows():
    """Rows pre-dating the ai_assessment column return NULL from TO_JSON; the
    mapper must yield an empty dict, not raise on json.loads(None).
    """
    cursor = MagicMock()
    snowflake = MagicMock()
    snowflake.get_connection.return_value.__enter__.return_value.cursor.return_value = cursor
    cursor.description = [
        ("BUILD_ID",),
        ("EXTRACTED_AT",),
        ("PIPELINE__ID",),
        ("PIPELINE__SLUG",),
        ("PIPELINE__NAME",),
        ("URL",),
        ("WEB_URL",),
        ("BUILD_NUMBER",),
        ("STATE",),
        ("BLOCKED",),
        ("CANCEL_REASON",),
        ("MESSAGE",),
        ("COMMIT",),
        ("BRANCH",),
        ("SOURCE",),
        ("CREATED_AT",),
        ("SCHEDULED_AT",),
        ("STARTED_AT",),
        ("FINISHED_AT",),
        ("AI_ASSESSMENT",),
    ]
    cursor.fetchall.side_effect = [
        [
            (
                "b1",
                _EXTRACTED_AT,
                "p1",
                "internal",
                "internal",
                "https://api/x",
                "https://buildkite/x",
                42,
                "passed",
                False,
                None,
                "msg",
                "abc",
                "main",
                "webhook",
                _EXTRACTED_AT,
                _EXTRACTED_AT,
                _EXTRACTED_AT,
                _EXTRACTED_AT,
                None,  # NULL ai_assessment (pre-migration row)
            )
        ],
        [],
    ]

    rebuilt = BuildkiteSQL(snowflake).get_builds()
    assert len(rebuilt) == 1
    assert rebuilt[0].ai_assessment == {}
