from dagster import Definitions
from dagster_open_platform.defs.sling_custom.assets import prod_sync_usage_metrics
from dagster_open_platform.defs.sling_custom.resources import cloud_prod_reporting_sling_resource
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git(prod_sync_usage_metrics),
    resources={
        "cloud_prod_reporting_sling": cloud_prod_reporting_sling_resource,
    },
)
