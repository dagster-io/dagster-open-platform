import os

import duckdb
import pytest
from dagster import AssetKey, Definitions
from dagster._core.definitions.materialize import materialize
from dagster_open_platform.resources.dlt_resource import (
    ConfigurableDltResource,
    DagsterDltTranslator,
    build_dlt_assets,
)

from .dlt_pipelines.duckdb_with_transformer import pipeline

EXAMPLE_PIPELINE_DUCKDB = "example_pipeline.duckdb"


@pytest.fixture
def _setup_and_teardown():
    yield
    if os.path.exists(EXAMPLE_PIPELINE_DUCKDB):
        os.remove(EXAMPLE_PIPELINE_DUCKDB)


def test_example_pipeline(_setup_and_teardown):
    resource = ConfigurableDltResource(
        source=pipeline(),
        pipeline_name="example_pipeline",
        dataset_name="example",
        destination="duckdb",
    )

    assets = build_dlt_assets(resource)

    res = materialize(assets)
    assert res.success

    with duckdb.connect(database=EXAMPLE_PIPELINE_DUCKDB, read_only=True) as conn:
        row = conn.execute("select count(*) from example.repos").fetchone()
        assert row and row[0] == 3

        row = conn.execute("select count(*) from example.repo_issues").fetchone()
        assert row and row[0] == 7


def test_names_do_not_conflict(_setup_and_teardown):
    assets1 = build_dlt_assets(
        ConfigurableDltResource(
            source=pipeline(),
            pipeline_name="example_pipeline",
            dataset_name="example1",
            destination="duckdb",
        ),
        name="example1",
    )
    assets2 = build_dlt_assets(
        ConfigurableDltResource(
            source=pipeline(),
            pipeline_name="example_pipeline",
            dataset_name="example2",
            destination="duckdb",
        ),
        name="example2",
    )

    # Should not throw: dagster._core.errors.DagsterInvalidDefinitionError
    assert Definitions(assets=[*assets1, *assets2])


def test_dlt_translator_get_asset_key():
    translator = DagsterDltTranslator()
    assert translator.get_asset_key("asset_key", "dataset_name") == AssetKey(
        "dlt_dataset_name_asset_key"
    )


def test_dlt_translator_get_deps_asset_keys():
    translator = DagsterDltTranslator()
    dlt_source = pipeline()
    deps = [translator.get_deps_asset_keys(resource) for resource in dlt_source.resources.values()]
    assert deps == [[AssetKey(["pipeline_repos"])], [AssetKey(["pipeline_repo_issues"])]]
