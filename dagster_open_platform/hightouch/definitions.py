from dagster import Definitions, EnvVar
from dagster_open_platform.hightouch import assets
from dagster_open_platform.hightouch.resources import ConfigurableHightouchResource

all_assets = [
    assets.hightouch_org_activity_monthly,
    assets.hightouch_org_info,
    assets.hightouch_null_contact_names,
    assets.hightouch_cloud_users,
]

hightouch_resource = ConfigurableHightouchResource(api_key=EnvVar("HIGHTOUCH_API_KEY"))

defs = Definitions(
    assets=all_assets,
    resources={
        "hightouch": hightouch_resource,
    },
)
