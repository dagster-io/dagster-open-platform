import dagster as dg
from dagster.components import definitions

from dagster_open_platform.defs.buildkite.assets import (
    buildkite_builds_hex_dashboard,
    buildkite_raw,
)
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[buildkite_raw, buildkite_builds_hex_dashboard],
        resources={
            "buildkite": BuildkiteResource(
                api_token=dg.EnvVar("SOURCES__BUILDKITE__BUILDKITE_API_TOKEN"), org_slug="dagster"
            ),
        },
    )
