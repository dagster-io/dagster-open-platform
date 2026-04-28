import json

from dagster_open_platform.defs.dbt.assets import (
    BACKFILL_SNOWFLAKE_WAREHOUSE,
    BACKFILL_STATEMENT_TIMEOUT_SECONDS,
    DbtConfig,
    _dbt_args,
    _is_dbt_backfill_run,
)
from dagster_open_platform.lib.dbt.backfill import DBT_BACKFILL_RUN_TAG, DBT_BACKFILL_RUN_TAG_VALUE


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


def test_is_dbt_backfill_run_uses_config_or_automation_tag() -> None:
    assert _is_dbt_backfill_run({}, DbtConfig(backfill=True))
    assert _is_dbt_backfill_run(
        {DBT_BACKFILL_RUN_TAG: DBT_BACKFILL_RUN_TAG_VALUE},
        DbtConfig(),
    )
    assert not _is_dbt_backfill_run({}, DbtConfig())
