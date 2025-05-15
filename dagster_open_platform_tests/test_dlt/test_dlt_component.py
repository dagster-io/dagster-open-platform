from pathlib import Path

import pytest
from dagster import AssetKey, AssetsDefinition
from dagster._core.test_utils import environ
from dagster.components.core.context import use_component_load_context
from dagster_open_platform.utils.components_test import build_component_and_defs
from dagster_open_platform_tests.utils import component_context


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_load_dlt_component(environment: str) -> None:
    ctx = component_context(Path("dlt/component"))
    with (
        environ(
            {
                "SOURCES__THINKIFIC__THINKIFIC__THINKIFIC_SUBDOMAIN": "foo",
                "SOURCES__THINKIFIC__THINKIFIC__THINKIFIC_API_KEY": "bar",
            }
        ),
        use_component_load_context(ctx),
    ):
        component, defs = build_component_and_defs(ctx)

        assets_def = next(iter(defs.assets or []))
        assert isinstance(assets_def, AssetsDefinition)

        assert assets_def.keys == {
            AssetKey(["dlt_thinkific_course_reviews"]),
            AssetKey(["dlt_thinkific_users"]),
            AssetKey(["dlt_thinkific_courses"]),
            AssetKey(["dlt_thinkific_enrollments"]),
        }
