import os
from pathlib import Path

import pytest
from dagster._core.test_utils import environ

FIVETRAN_API_SECRET_ENV_VAR = "FIVETRAN_API_SECRET"
FIVETRAN_API_KEY_ENV_VAR = "FIVETRAN_API_KEY"


@pytest.mark.env_bk
def test_dop_code_location():
    """Ensure that the code location can be loaded without error."""
    PLACEHOLDER_ENV_VAR_VALUE = "x"
    # Empty values because we don't need to run the pipelines, just load them

    from dagster_open_platform.resources import dbt_resource

    dbt_resource.cli(["deps"]).wait()
    dbt_resource.cli(["parse"], target_path=Path("target")).wait()

    manifest_path = (
        Path(__file__).parent.parent / "dagster_open_platform_dbt" / "target" / "manifest.json"
    )

    assert manifest_path.exists()

    with environ(
        {
            "FIVETRAN_API_SECRET": os.getenv(FIVETRAN_API_SECRET_ENV_VAR),
            "FIVETRAN_API_KEY": os.getenv(FIVETRAN_API_KEY_ENV_VAR),
            "THINKIFIC_SUBDOMAIN": PLACEHOLDER_ENV_VAR_VALUE,
            "THINKIFIC_API_KEY": PLACEHOLDER_ENV_VAR_VALUE,
            "SOURCES__HUBSPOT__API_KEY": PLACEHOLDER_ENV_VAR_VALUE,
            "SOURCES__GITHUB__ACCESS_TOKEN": PLACEHOLDER_ENV_VAR_VALUE,
        }
    ):
        from dagster_open_platform.definitions import defs

        assert len(defs.get_asset_graph().all_asset_keys) > 0
