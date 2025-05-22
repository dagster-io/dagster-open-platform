from dagster import ConfigurableResource, Definitions, EnvVar
from dagster.components import definitions
from dagster_hightouch.resources import HightouchOutput, HightouchResource


class ConfigurableHightouchResource(ConfigurableResource):
    """A thin wrapper around the Hightouch Resource until someone updates it
    to use ConfigurableResources and assets.
    """

    api_key: str

    def sync_and_poll(self, sync_id: str) -> HightouchOutput:
        hightouch_resource = HightouchResource(api_key=self.api_key)
        return hightouch_resource.sync_and_poll(sync_id)


@definitions
def defs() -> Definitions:
    return Definitions(
        resources={"hightouch": ConfigurableHightouchResource(api_key=EnvVar("HIGHTOUCH_API_KEY"))},
    )
