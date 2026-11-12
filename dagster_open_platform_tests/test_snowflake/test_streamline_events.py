import re
from datetime import date

from dagster_open_platform.defs.snowflake.py.assets.streamline_events import (
    LOAD_LOOKBACK_DAYS,
    _recent_files_pattern,
)


def _matches(pattern: str, path: str) -> bool:
    return re.fullmatch(pattern, path) is not None


def test_pattern_covers_window_across_month_boundary() -> None:
    pattern = _recent_files_pattern(date(2026, 11, 3), days=5)

    assert _matches(pattern, "123/456/2026/11/03/23/987.parquet")
    assert _matches(pattern, "123/456/2026/10/30/00/1.parquet")
    assert not _matches(pattern, "123/456/2026/10/29/23/1.parquet")
    assert not _matches(pattern, "123/456/2026/11/04/00/1.parquet")


def test_pattern_matches_full_stage_url() -> None:
    pattern = _recent_files_pattern(date(2026, 11, 12))

    assert _matches(
        pattern,
        "s3://cloud-prod-streamline-event-stream/streamline-events/123/456/2026/11/12/16/42.parquet",
    )


def test_pattern_ignores_non_parquet_files() -> None:
    pattern = _recent_files_pattern(date(2026, 11, 12))

    assert not _matches(pattern, "123/456/2026/11/12/16/42.json")


def test_default_window_is_lookback_days() -> None:
    pattern = _recent_files_pattern(date(2026, 11, 12))

    assert pattern.count("|") == LOAD_LOOKBACK_DAYS - 1
    assert _matches(pattern, "1/2/2026/11/03/00/1.parquet")
    assert not _matches(pattern, "1/2/2026/11/02/23/1.parquet")
