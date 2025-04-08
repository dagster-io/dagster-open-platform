import dagster as dg
from dagster_open_platform.defs.statsig.assets import org_performance_metrics, user_activity_metrics
from dagster_open_platform.defs.statsig.resources import DatadogMetricsResource, StatsigResource
from dagster_open_platform.defs.statsig.schedules import statsig_upload_schedule
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

statsig = StatsigResource(
    api_key=dg.EnvVar("STATSIG_API_KEY"),
)

datadog = DatadogMetricsResource(
    api_key=dg.EnvVar("DATADOG_API_KEY"),
    app_key=dg.EnvVar("DATADOG_APP_KEY"),
)

defs = dg.Definitions(
    assets=add_code_references_and_link_to_git([org_performance_metrics, user_activity_metrics]),
    resources={"statsig": statsig, "datadog": datadog},
    schedules=[statsig_upload_schedule],
)
