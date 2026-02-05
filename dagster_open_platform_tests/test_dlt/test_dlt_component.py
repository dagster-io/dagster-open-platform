from pathlib import Path

import pytest
from dagster import AssetKey, AssetsDefinition, get_component_defs_within_project
from dagster._core.test_utils import environ


@pytest.mark.parametrize("environment", ["dev", "prod"])
def test_load_dlt_component(environment: str) -> None:
    with environ(
        {
            "SOURCES__THINKIFIC__THINKIFIC__THINKIFIC_SUBDOMAIN": "foo",
            "SOURCES__THINKIFIC__THINKIFIC__THINKIFIC_API_KEY": "bar",
        }
    ):
        _component, defs = get_component_defs_within_project(
            project_root=Path(__file__).parent.parent.parent,
            component_path="dlt/component",
        )

        assets_def = next(iter(defs.assets or []))
        assert isinstance(assets_def, AssetsDefinition)

        assert assets_def.keys == {
            AssetKey(["dlt_thinkific_course_reviews"]),
            AssetKey(["dlt_thinkific_users"]),
            AssetKey(["dlt_thinkific_courses"]),
            AssetKey(["dlt_thinkific_enrollments"]),
        }
