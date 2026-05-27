"""Tests for _epoch_to_iso timestamp parsing in claude assets.

The Anthropic API has historically returned timestamps as both Unix epoch integers/strings
and ISO 8601 strings. Both formats must be handled correctly.
"""

import pytest
from dagster_open_platform.defs.claude.assets import _epoch_to_iso


@pytest.mark.parametrize(
    "ts,expected",
    [
        # None passthrough
        (None, None),
        # Integer epoch
        (0, "1970-01-01T00:00:00+00:00"),
        (1739491267, "2025-02-14T00:01:07+00:00"),
        # String epoch (format returned before API change)
        ("0", "1970-01-01T00:00:00+00:00"),
        ("1739491267", "2025-02-14T00:01:07+00:00"),
        # ISO 8601 with Z suffix (format returned after API change)
        ("2026-02-13T23:01:07.217528Z", "2026-02-13T23:01:07.217528+00:00"),
        ("2026-02-13T23:01:07Z", "2026-02-13T23:01:07+00:00"),
        # ISO 8601 with explicit offset
        ("2026-02-13T23:01:07+00:00", "2026-02-13T23:01:07+00:00"),
    ],
)
def test_epoch_to_iso(ts, expected):
    assert _epoch_to_iso(ts) == expected
