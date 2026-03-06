import dagster as dg
from dagster.components import definitions
from dagster_open_platform.defs.buildkite.assets import buildkite_daily_analysis
from dagster_open_platform.defs.buildkite.resources import BuildkiteResource
from dagster_open_platform.defs.claude.resources import claude_resource
from dagster_slack import SlackResource


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[buildkite_daily_analysis],
        resources={
            "buildkite": BuildkiteResource(
                api_token=dg.EnvVar("SOURCES__BUILDKITE__BUILDKITE_API_TOKEN"), org_slug="dagster"
            ),
            "claude": claude_resource,
            "slack_buildkite": SlackResource(token=dg.EnvVar("SLACK_ELEMENTL_ETL_BOT_TOKEN")),
        },
    )
