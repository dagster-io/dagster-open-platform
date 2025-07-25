import itertools
import os
import sys
from collections.abc import Iterator
from typing import Any, Callable

import pytest
from dagster import (
    AssetsDefinition,
    AssetSpec,
    _check as check,
)
from dagster._core.definitions.metadata.source_code import (
    CodeReferencesMetadataSet,
    LocalFileCodeReference,
    UrlCodeReference,
)
from dagster._core.test_utils import environ

FIVETRAN_API_SECRET_ENV_VAR = "FIVETRAN_API_SECRET"
FIVETRAN_API_KEY_ENV_VAR = "FIVETRAN_API_KEY"


@pytest.fixture
def reload_dop_modules() -> Callable[[], None]:
    def _reload() -> None:
        modules = [*sys.modules.values()]
        for module in modules:
            if "dagster_open_platform" in module.__name__:
                sys.modules.pop(module.__name__)

    return _reload


@pytest.fixture
@pytest.mark.parametrize("cloud_env", [True, False])
def prepare_dop_environment(
    reload_dop_modules: Callable[[], None], cloud_env: bool
) -> Iterator[None]:
    """Sets up the requisite environment to run DOP."""
    PLACEHOLDER_ENV_VAR_VALUE = "x"
    # Empty values because we don't need to run the pipelines, just load them

    from dagster_open_platform.defs.dbt.resources import dagster_open_platform_dbt_project

    dbt_project = dagster_open_platform_dbt_project()
    dbt_project.preparer.prepare(dbt_project)
    manifest_path = dbt_project.manifest_path

    assert manifest_path.exists()

    if not cloud_env:
        if "DAGSTER_CLOUD_DEPLOYMENT_NAME" in os.environ:
            del os.environ["DAGSTER_CLOUD_DEPLOYMENT_NAME"]
        if "DAGSTER_CLOUD_GIT_URL" in os.environ:
            del os.environ["DAGSTER_CLOUD_GIT_URL"]
        if "DAGSTER_CLOUD_GIT_BRANCH" in os.environ:
            del os.environ["DAGSTER_CLOUD_GIT_BRANCH"]

    with environ(
        {
            "FIVETRAN_ACCOUNT_ID": os.getenv("FIVETRAN_ACCOUNT_ID", "corpse_deliberation"),
            "FIVETRAN_API_SECRET": check.not_none(
                os.getenv(FIVETRAN_API_SECRET_ENV_VAR),
                f"{FIVETRAN_API_SECRET_ENV_VAR} is not set",
            ),
            "FIVETRAN_API_KEY": check.not_none(
                os.getenv(FIVETRAN_API_KEY_ENV_VAR),
                f"{FIVETRAN_API_KEY_ENV_VAR} is not set",
            ),
            "COMMON_ROOM_BUCKET": PLACEHOLDER_ENV_VAR_VALUE,
            "COMMON_ROOM_STORAGE_INTEGRATION": PLACEHOLDER_ENV_VAR_VALUE,
            "THINKIFIC_SUBDOMAIN": PLACEHOLDER_ENV_VAR_VALUE,
            "THINKIFIC_API_KEY": PLACEHOLDER_ENV_VAR_VALUE,
            "SOURCES__HUBSPOT__API_KEY": PLACEHOLDER_ENV_VAR_VALUE,
            "SOURCES__GITHUB__ACCESS_TOKEN": PLACEHOLDER_ENV_VAR_VALUE,
            "SOURCES__BUILDKITE__BUILDKITE_API_TOKEN": PLACEHOLDER_ENV_VAR_VALUE,
            **(
                {
                    "DAGSTER_CLOUD_DEPLOYMENT_NAME": "prod",
                    "DAGSTER_CLOUD_GIT_URL": "https://github.com/dagster-io/internal",
                    "DAGSTER_CLOUD_GIT_BRANCH": "master",
                }
                if cloud_env
                else {}
            ),
        }
    ):
        # some DOP behavior is determined by env vars at import time, so we need to reload the modules
        # between tests
        reload_dop_modules()
        yield


@pytest.mark.skip(reason="Skipping code references test until components implement them")
@pytest.mark.env_bk
@pytest.mark.parametrize("cloud_env", [True, False])
def test_dop_code_location(prepare_dop_environment, cloud_env: bool) -> None:
    from dagster_open_platform.definitions import defs

    resolved_defs = defs()
    assert len(resolved_defs.resolve_asset_graph().get_all_asset_keys()) > 0
    assert resolved_defs.assets is not None

    for asset in resolved_defs.assets:
        if not isinstance(asset, AssetsDefinition):
            continue
        for key in asset.keys:
            code_references_meta = CodeReferencesMetadataSet.extract(asset.metadata_by_key[key])
            assert code_references_meta.code_references is not None, f"{key} has no code references"

            assert len(code_references_meta.code_references.code_references) > 0
            is_ref = [
                isinstance(ref, UrlCodeReference if cloud_env else LocalFileCodeReference)
                for ref in code_references_meta.code_references.code_references
            ]
            assert all(is_ref), (
                f"{key} has incorrect code references: {code_references_meta.code_references.code_references}"
            )


# We ignore certain metadata keys which are not reliably regenerated in each test run
IGNORED_METADATA_KEYS = {"dagster_dbt/manifest", "dagster/code_references"}
IGNORED_METADATA_PREFIXES = {"dagster_dlt"}


def _asset_spec_data(asset: AssetSpec) -> dict[str, Any]:
    """Gets formatted, sorted representation of an asset spec for snapshot testing."""
    return {
        "key": asset.key.to_user_string(),
        "deps": sorted(asset.deps, key=lambda x: x.asset_key.to_user_string()),
        "description": asset.description,
        "metadata": {
            k: v
            for k, v in asset.metadata.items()
            if k not in IGNORED_METADATA_KEYS
            and not any(k.startswith(prefix) for prefix in IGNORED_METADATA_PREFIXES)
        },
        "group_name": asset.group_name,
        "owners": asset.owners,
        "tags": asset.tags,
        **(
            {"automation_condition": asset.automation_condition}
            if asset.automation_condition
            else {}
        ),
    }


@pytest.mark.skipif(
    os.environ.get("DOP_PYTEST_FULL") != "1",
    reason="Snapshot tests are opt-in using `--run-skipped`",
)
@pytest.mark.parametrize("cloud_env", [True])
def test_assets_snapshot(prepare_dop_environment, cloud_env: bool, snapshot) -> None:
    """Ensures that the asset specs are stable across test runs, and don't unexpectedly change.

    To run locally, you will need env vars configured as in the README.
    """
    from dagster_open_platform.definitions import defs

    resolved_defs = defs()

    assert len(resolved_defs.resolve_asset_graph().get_all_asset_keys()) > 0
    assert resolved_defs.assets is not None

    # flatten
    specs = sorted(
        itertools.chain.from_iterable(
            asset.specs
            for asset in resolved_defs.get_repository_def().asset_graph.assets_defs
            if isinstance(asset, (AssetsDefinition, AssetSpec))
        ),
        key=lambda x: x.key.to_user_string(),
    )
    # Separately validate that the asset keys are stable across test runs, since this is easier to
    # reason about than the full spec data.
    spec_keys = [spec.key.to_user_string() for spec in specs]
    snapshot.assert_match(spec_keys)

    spec_data = [_asset_spec_data(spec) for spec in specs]
    snapshot.assert_match(spec_data)


@pytest.mark.skipif(
    os.environ.get("DOP_PYTEST_FULL") != "1",
    reason="Snapshot tests are opt-in using `--run-skipped`",
)
@pytest.mark.parametrize("cloud_env", [True])
def test_jobs_snapshot(prepare_dop_environment, cloud_env: bool, snapshot) -> None:
    """Ensures that job list and job asset selection are stable across test runs, and don't
    unexpectedly change.
    """
    from dagster_open_platform.definitions import defs

    repo_def = defs().get_repository_def()

    job_data = [
        {
            "name": job.name,
            "asset_selection": sorted(
                k.to_user_string() for k in job.asset_layer.asset_graph.get_all_asset_keys()
            )
            if job.asset_layer
            else None,
        }
        for job in sorted(
            repo_def.get_all_jobs(),
            key=lambda j: j.name,
        )
    ]
    snapshot.assert_match(job_data)


@pytest.mark.skipif(
    os.environ.get("DOP_PYTEST_FULL") != "1",
    reason="Snapshot tests are opt-in using `--run-skipped`",
)
@pytest.mark.parametrize("cloud_env", [True])
def test_schedules_snapshot(prepare_dop_environment, cloud_env: bool, snapshot) -> None:
    """Ensures that schedule list and schedule selection are stable across test runs, and don't
    unexpectedly change.
    """
    from dagster_open_platform.definitions import defs

    repo_def = defs().get_repository_def()
    schedule_data = [
        {
            "name": schedule.name,
            "job_name": schedule.job_name,
            "cron": schedule.cron_schedule,
        }
        for schedule in sorted(
            repo_def.schedule_defs,
            key=lambda s: s.name,
        )
    ]

    snapshot.assert_match(schedule_data)
