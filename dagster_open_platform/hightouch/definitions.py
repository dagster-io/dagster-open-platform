from dagster import Definitions, EnvVar
from dagster_open_platform.hightouch import assets
from dagster_open_platform.hightouch.resources import ConfigurableHightouchResource

from ..utils.source_code import add_code_references_and_link_to_git

all_assets = [
    assets.hightouch_org_activity_monthly,
    assets.hightouch_org_info,
    assets.hightouch_null_contact_names,
    assets.hightouch_cloud_users,
]

hightouch_resource = ConfigurableHightouchResource(api_key=EnvVar("HIGHTOUCH_API_KEY"))

defs = Definitions(
    assets=add_code_references_and_link_to_git(all_assets),
    resources={
        "hightouch": hightouch_resource,
    },
)
