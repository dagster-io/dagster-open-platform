from collections.abc import Iterable
from pathlib import Path
from typing import Any

from dagster import AssetKey, Definitions
from dagster.components import definitions
from dagster_open_platform.utils.environment_helpers import (
    get_environment,
    get_schema_for_environment,
)
from dagster_sling import DagsterSlingTranslator, SlingResource, sling_assets

reporting_db_config_dir = Path(__file__).parent / "configs" / "reporting_db"


class CustomSlingTranslatorEgress(DagsterSlingTranslator):
    def get_group_name(self, stream_definition):
        return "sling_egress"

    def get_deps_asset_key(self, stream_definition) -> Iterable[AssetKey]:
        stream_asset_key = next(iter(super().get_deps_asset_key(stream_definition)))
        db, schema, table = stream_asset_key.path
        db = "sandbox" if get_environment() == "LOCAL" else "purina"
        schema = get_schema_for_environment(schema)
        return [AssetKey([db, schema, table])]


@sling_assets(
    replication_config=reporting_db_config_dir / "salesforce_contract_info.yaml",
    dagster_sling_translator=CustomSlingTranslatorEgress(),
)
def salesforce_contract_info(context, embedded_elt: SlingResource) -> Iterable[Any]:
    yield from (embedded_elt.replicate(context=context).fetch_column_metadata().fetch_row_count())


@definitions
def defs():
    return Definitions(
        assets=[
            salesforce_contract_info,
        ]
    )
