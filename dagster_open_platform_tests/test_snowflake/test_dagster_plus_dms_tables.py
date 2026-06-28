"""Pure-Python coverage for the Dagster Plus DMS COPY INTO asset generation.

These tests exercise asset-key and stage-path construction only; they do not
touch Snowflake.
"""

from contextlib import contextmanager

from dagster import AssetKey, materialize_to_memory
from dagster._core.test_utils import environ
from dagster_open_platform.defs.snowflake.py.assets import dagster_plus_dms_tables as mod


def _expected_dest_tables() -> set[str]:
    dest: set[str] = {f"{mod._TABLE_PREFIX}{t}" for t in mod._DMS_TABLES}  # noqa: SLF001
    for shard in mod._SHARD_PREFIXES:  # noqa: SLF001
        for table in (*mod._SHARDED_TABLES, *mod._SHARDED_CDC_ONLY_TABLES):  # noqa: SLF001
            dest.add(f"{mod._TABLE_PREFIX}{table}__{shard}")  # noqa: SLF001
    for shard in mod._JOB_TICKS_PREFIXES:  # noqa: SLF001
        dest.add(f"{mod._TABLE_PREFIX}job_ticks__{shard}")  # noqa: SLF001
    # One deduplicated "current" dynamic table per COPY INTO table above.
    return dest | {f"{d}__current" for d in dest}


def test_asset_keys_cover_unsharded_and_per_shard_tables() -> None:
    # Tests run in the LOCAL environment, so the schema resolves to "dev".
    schema = mod._dms_schema_from_env()  # noqa: SLF001
    assert schema == "dev"

    keys = [next(iter(a.keys)) for a in mod.dagster_plus_dms_table_assets]

    # No duplicate destination tables across unsharded + sharded + job_ticks.
    assert len(keys) == len(set(keys))

    expected = {AssetKey([mod._AWS_DB, schema, dest]) for dest in _expected_dest_tables()}  # noqa: SLF001
    assert set(keys) == expected

    # Spot-check the per-shard separation that motivates this design.
    assert AssetKey(["aws", "dev", "dagster_plus__jobs__shard0"]) in expected
    assert AssetKey(["aws", "dev", "dagster_plus__jobs__shard1"]) in expected
    assert AssetKey(["aws", "dev", "dagster_plus__job_ticks__shard1"]) in expected


def test_build_copy_into_asset_uses_prod_schema_and_stage_path() -> None:
    with environ({"DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod"}):
        asset = mod._build_copy_into_asset(  # noqa: SLF001
            stage_subpath="public_shard0/jobs",
            dest_table="dagster_plus__jobs__shard0",
            description_source="public_shard0.jobs",
        )

    assert next(iter(asset.keys)) == AssetKey(["aws", "cloud_prod", "dagster_plus__jobs__shard0"])
    spec = next(iter(asset.specs))
    assert "public_shard0.jobs" in (spec.description or "")


class _FakeCursor:
    def __init__(self) -> None:
        self.sql: list[str] = []

    def execute(self, sql: str) -> None:
        self.sql.append(sql)

    def fetchall(self):
        return [("exists",)]  # non-empty: skip CREATE TABLE; COPY yields no rows_loaded


class _FakeConn:
    def __init__(self, cursor: object) -> None:
        self._cursor = cursor

    def cursor(self) -> object:
        return self._cursor


class _FakeSnowflake:
    # Accepts any cursor-like fake (_FakeCursor for the COPY INTO tests, _DynCursor
    # for the dynamic-table tests); the asset code only duck-types the cursor.
    def __init__(self, cursor: object) -> None:
        self._conn = _FakeConn(cursor)

    @contextmanager
    def get_connection(self):
        yield self._conn


def _run(warehouse) -> list[str]:
    cursor = _FakeCursor()
    asset = mod._build_copy_into_asset(  # noqa: SLF001
        stage_subpath="public_shard0/jobs",
        dest_table="dagster_plus__jobs__shard0",
        description_source="public_shard0.jobs",
        warehouse=warehouse,
    )
    materialize_to_memory([asset], resources={"snowflake": _FakeSnowflake(cursor)})
    return cursor.sql


def test_warehouse_override_emits_use_warehouse() -> None:
    sql = _run("L_WAREHOUSE")
    assert "USE WAREHOUSE L_WAREHOUSE;" in sql


def test_no_warehouse_skips_use_warehouse() -> None:
    assert not any(s.startswith("USE WAREHOUSE") for s in _run(None))


class _DynCursor:
    """Fake cursor for the dynamic-table asset.

    Answers the existence/count/warehouse probes the refresh issues so we can
    assert which warehouse it resolves for a given table state.
    """

    def __init__(self, *, dynamic_exists: bool, counts, session_wh: str = "SESSION_WH") -> None:
        self.sql: list[str] = []
        self._dynamic_exists = dynamic_exists
        self._counts = list(counts)
        self._session_wh = session_wh
        self._last = ""

    def execute(self, sql: str) -> None:
        self.sql.append(sql)
        self._last = sql

    def fetchall(self):
        if self._last.lstrip().startswith("SHOW DYNAMIC TABLES"):
            return [("dt",)] if self._dynamic_exists else []
        # SHOW TABLES LIKE <base>: base table always exists in these tests.
        return [("base",)]

    def fetchone(self):
        last = self._last
        if "CURRENT_WAREHOUSE" in last:
            return (self._session_wh,)
        if last.lstrip().upper().startswith("SELECT COUNT(*)"):
            return (self._counts.pop(0),)
        # DYNAMIC_TABLE_REFRESH_HISTORY row.
        return ("REFRESH", "SUCCEEDED")


def _run_dynamic(*, dynamic_exists: bool, counts, warehouse=None) -> list[str]:
    cursor = _DynCursor(dynamic_exists=dynamic_exists, counts=counts)
    asset = mod._build_dynamic_table_asset(  # noqa: SLF001
        base_dest_table="dagster_plus__jobs__shard0",
        partition_by=["id"],
        description_source="public_shard0.jobs",
        warehouse=warehouse,
    )
    materialize_to_memory([asset], resources={"snowflake": _FakeSnowflake(cursor)})
    return cursor.sql


_CURRENT = "aws.dev.dagster_plus__jobs__shard0__current"


def test_dynamic_table_first_refresh_creates_with_l_warehouse() -> None:
    # Table does not exist yet: created on L_WAREHOUSE, no warehouse re-point.
    sql = _run_dynamic(dynamic_exists=False, counts=[10])
    assert any(
        "CREATE DYNAMIC TABLE IF NOT EXISTS" in s and "WAREHOUSE = L_WAREHOUSE" in s for s in sql
    )
    assert not any("SET WAREHOUSE" in s for s in sql)


def test_dynamic_table_existing_empty_uses_l_warehouse() -> None:
    # Table exists but is empty: still a first load, so re-point to L_WAREHOUSE.
    sql = _run_dynamic(dynamic_exists=True, counts=[0, 10])
    assert f"ALTER DYNAMIC TABLE {_CURRENT} SET WAREHOUSE = L_WAREHOUSE;" in sql


def test_dynamic_table_existing_populated_uses_session_warehouse() -> None:
    # Table already has rows: drop back to the session warehouse.
    sql = _run_dynamic(dynamic_exists=True, counts=[5, 5])
    assert f"ALTER DYNAMIC TABLE {_CURRENT} SET WAREHOUSE = SESSION_WH;" in sql


def test_dynamic_table_configured_warehouse_always_used() -> None:
    # Pinned tables keep L_WAREHOUSE even once populated.
    sql = _run_dynamic(dynamic_exists=True, counts=[5, 5], warehouse="L_WAREHOUSE")
    assert f"ALTER DYNAMIC TABLE {_CURRENT} SET WAREHOUSE = L_WAREHOUSE;" in sql
