import os

import pytest


@pytest.fixture(autouse=True)
def set_env_variable():
    os.environ["THINKIFIC_SUBDOMAIN"] = "xxx"
    os.environ["THINKIFIC_API_KEY"] = "xxx"
    os.environ["SOURCES__HUBSPOT__API_KEY"] = "xxx"
    os.environ["SOURCES__GITHUB__ACCESS_TOKEN"] = "xxx"
    os.environ["SOURCES__BUILDKITE__BUILDKITE_API_TOKEN"] = "xxx"


def test_configuration_yaml_loads():
    from dagster_open_platform.assets.dlt import dlt_configuration

    assert "sources" in dlt_configuration
    dlt_configuration_sources = dlt_configuration.get("sources")
    assert "github" in dlt_configuration_sources
    dlt_configuration_sources_github = dlt_configuration_sources.get("github")
    assert "repositories" in dlt_configuration_sources_github
