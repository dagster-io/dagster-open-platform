from pathlib import Path

from dagster import AssetKey
from dagster_embedded_elt.sling.asset_decorator import sling_assets
from dagster_embedded_elt.sling.dagster_sling_translator import DagsterSlingTranslator
from dagster_embedded_elt.sling.resources import SlingResource

from ..utils.environment_helpers import get_environment, get_schema_for_environment

config_dir = Path(__file__).parent.parent / "configs" / "sling" / "reporting_db"


class CustomSlingTranslator(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "sling_egress"

    def get_deps_asset_key(self, stream_definition):
        stream_asset_key = super().get_deps_asset_key(stream_definition)[0]
        db, schema, table = stream_asset_key[0]
        db = "sandbox" if get_environment() == "LOCAL" else "purina"
        schema = get_schema_for_environment(schema)
        return AssetKey([db, schema, table])


@sling_assets(
    replication_config=config_dir / "salesforce_contract_info.yaml",
    dagster_sling_translator=CustomSlingTranslator(),
)
def salesforce_contract_info(context, embedded_elt: SlingResource):
    yield from embedded_elt.replicate(context=context)
