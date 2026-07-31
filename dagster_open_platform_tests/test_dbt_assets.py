import datetime
import json

import dagster as dg
from dagster_open_platform.defs.dbt.assets import (
    BACKFILL_SNOWFLAKE_WAREHOUSE,
    BACKFILL_STATEMENT_TIMEOUT_SECONDS,
    DbtConfig,
    _dbt_args,
    _is_dbt_backfill_run,
    _partitioned_dbt_vars,
    _region_vars,
)
from dagster_open_platform.lib.dbt.backfill import DBT_BACKFILL_RUN_TAG, DBT_BACKFILL_RUN_TAG_VALUE
from dagster_open_platform_eu.defs.dbt.assets import REGION as EU_REGION

PARTITION_WINDOW = dg.TimeWindow(
    start=datetime.datetime(2026, 1, 2, tzinfo=datetime.timezone.utc),
    end=datetime.datetime(2026, 1, 3, tzinfo=datetime.timezone.utc),
)


def _dbt_vars(args: list[str]) -> dict[str, object]:
    return json.loads(args[args.index("--vars") + 1])


def test_dbt_args_omits_vars_for_normal_non_partitioned_run() -> None:
    assert _dbt_args("build", DbtConfig()) == ["build"]


def test_dbt_args_preserves_partition_vars_for_normal_partitioned_run() -> None:
    args = _dbt_args(
        "build",
        DbtConfig(),
        {
            "min_date": "2026-01-01T00:00:00",
            "max_date": "2026-01-02T00:00:00",
        },
    )

    assert args[:2] == ["build", "--vars"]
    assert _dbt_vars(args) == {
        "min_date": "2026-01-01T00:00:00",
        "max_date": "2026-01-02T00:00:00",
    }


def test_dbt_args_adds_backfill_vars() -> None:
    args = _dbt_args("build", DbtConfig(backfill=True), {"min_date": "2026-01-01T00:00:00"})

    assert args[:2] == ["build", "--vars"]
    assert _dbt_vars(args) == {
        "min_date": "2026-01-01T00:00:00",
        "backfill": True,
        "backfill_snowflake_warehouse": BACKFILL_SNOWFLAKE_WAREHOUSE,
        "backfill_statement_timeout_seconds": BACKFILL_STATEMENT_TIMEOUT_SECONDS,
    }


def test_dbt_args_allows_full_refresh_backfill() -> None:
    args = _dbt_args("build", DbtConfig(full_refresh=True, backfill=True))

    assert args[:3] == ["build", "--full-refresh", "--vars"]
    assert _dbt_vars(args) == {
        "backfill": True,
        "backfill_snowflake_warehouse": BACKFILL_SNOWFLAKE_WAREHOUSE,
        "backfill_statement_timeout_seconds": BACKFILL_STATEMENT_TIMEOUT_SECONDS,
    }


def test_dbt_args_can_force_backfill_from_automation_tag() -> None:
    args = _dbt_args("build", DbtConfig(), backfill=True)

    assert args[:2] == ["build", "--vars"]
    assert _dbt_vars(args) == {
        "backfill": True,
        "backfill_snowflake_warehouse": BACKFILL_SNOWFLAKE_WAREHOUSE,
        "backfill_statement_timeout_seconds": BACKFILL_STATEMENT_TIMEOUT_SECONDS,
    }


def test_dbt_args_appends_exclude() -> None:
    assert _dbt_args("build", DbtConfig(), exclude="tag:us_only") == [
        "build",
        "--exclude",
        "tag:us_only",
    ]


def test_dbt_args_appends_exclude_after_vars() -> None:
    args = _dbt_args(
        "build",
        DbtConfig(),
        {
            "min_date": "2026-01-01T00:00:00",
            "max_date": "2026-01-02T00:00:00",
        },
        exclude="tag:us_only",
    )

    assert args[:2] == ["build", "--vars"]
    assert args[-2:] == ["--exclude", "tag:us_only"]
    assert args.index("--exclude") > args.index("--vars")
    assert _dbt_vars(args) == {
        "min_date": "2026-01-01T00:00:00",
        "max_date": "2026-01-02T00:00:00",
    }


def test_is_dbt_backfill_run_uses_config_or_automation_tag() -> None:
    assert _is_dbt_backfill_run({}, DbtConfig(backfill=True))
    assert _is_dbt_backfill_run(
        {DBT_BACKFILL_RUN_TAG: DBT_BACKFILL_RUN_TAG_VALUE},
        DbtConfig(),
    )
    assert not _is_dbt_backfill_run({}, DbtConfig())


def test_region_vars_omitted_for_the_default_us_deployment() -> None:
    # The US deployment takes the `region` default from dbt_project.yml, so its
    # invocations stay byte-identical to what they were before regions existed.
    assert _region_vars(None) == {}


def test_region_vars_pins_the_eu_deployment() -> None:
    assert _region_vars(EU_REGION) == {"region": "eu"}


def test_partitioned_dbt_vars_carry_the_region_alongside_the_window() -> None:
    assert _partitioned_dbt_vars(EU_REGION, DbtConfig(), PARTITION_WINDOW) == {
        "region": "eu",
        "min_date": "2026-01-01T21:00:00+00:00",
        "max_date": "2026-01-04T00:00:00+00:00",
    }


def test_partitioned_dbt_vars_keep_the_region_on_full_refresh() -> None:
    # Regression: a full refresh drops the date window, and dropping the region
    # with it would make the EU deployment compile the US model bodies against
    # sources that do not exist in the EU Snowflake account.
    assert _partitioned_dbt_vars(EU_REGION, DbtConfig(full_refresh=True), PARTITION_WINDOW) == {
        "region": "eu"
    }


def test_partitioned_dbt_vars_omit_vars_entirely_for_a_us_full_refresh() -> None:
    assert _partitioned_dbt_vars(None, DbtConfig(full_refresh=True), PARTITION_WINDOW) == {}
