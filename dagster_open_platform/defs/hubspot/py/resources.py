from dagster import ConfigurableResource, Definitions, EnvVar
from dagster.components import definitions


class HubSpotResource(ConfigurableResource):
    access_token: str


@definitions
def defs():
    return Definitions(
        resources={"hubspot": HubSpotResource(access_token=EnvVar("SOURCES__HUBSPOT__API_KEY"))},
    )
