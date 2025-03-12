import dagster as dg
from dagster_open_platform.statsig.assets import org_performance_daily, user_activity_daily
from dagster_open_platform.statsig.resources import DatadogMetricsResource, StatsigResource

from ..utils.source_code import add_code_references_and_link_to_git

statsig = StatsigResource(
    api_key=dg.EnvVar("STATSIG_API_KEY"),
)

datadog = DatadogMetricsResource(
    api_key=dg.EnvVar("DATADOG_API_KEY"),
    app_key=dg.EnvVar("DATADOG_APP_KEY"),
)

defs = dg.Definitions(
    assets=add_code_references_and_link_to_git([user_activity_daily, org_performance_daily]),
    resources={"statsig": statsig, "datadog": datadog},
)
