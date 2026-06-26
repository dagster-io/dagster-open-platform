"""Pure-Python coverage for the Dagster Plus DMS COPY INTO asset generation.

These tests exercise asset-key and stage-path construction only; they do not
touch Snowflake.
"""

from dagster import AssetKey
from dagster._core.test_utils import environ
from dagster_open_platform.defs.snowflake.py.assets import dagster_plus_dms_tables as mod


def _expected_dest_tables() -> set[str]:
    dest: set[str] = {f"{mod._TABLE_PREFIX}{t}" for t in mod._DMS_TABLES}  # noqa: SLF001
    for shard in mod._SHARD_PREFIXES:  # noqa: SLF001
        for table in (*mod._SHARDED_TABLES, *mod._SHARDED_CDC_ONLY_TABLES):  # noqa: SLF001
            dest.add(f"{mod._TABLE_PREFIX}{table}__{shard}")  # noqa: SLF001
    for shard in mod._JOB_TICKS_PREFIXES:  # noqa: SLF001
        dest.add(f"{mod._TABLE_PREFIX}job_ticks__{shard}")  # noqa: SLF001
    return dest


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
