from typing import Optional

import dagster as dg
from dagster_dbt import get_asset_key_for_model
from dagster_omni import OmniComponent
from dagster_omni.objects import OmniDocument, OmniQuery
from dagster_omni.translation import OmniTranslatorData


class DOPOmniComponent(OmniComponent):
    """Customized OmniComponent that associates queries with dbt models."""

    def get_asset_spec(
        self, context: dg.ComponentLoadContext, data: OmniTranslatorData
    ) -> Optional[dg.AssetSpec]:
        from dagster_open_platform.defs.dbt.assets import get_dbt_non_partitioned_models

        base_spec = super().get_asset_spec(context, data)
        if not base_spec:
            return None

        # filter out documents that are in the archive or playground groups
        if isinstance(data.obj, OmniDocument) and base_spec.group_name in [
            None,
            "archive",
            "playground",
        ]:
            return None
        elif isinstance(data.obj, OmniQuery):
            # attempt to map the query key to a dbt model
            try:
                key = get_asset_key_for_model(
                    [get_dbt_non_partitioned_models()],
                    data.obj.query_config.table.split("__")[-1],
                )
                return base_spec.replace_attributes(key=key)
            # if no matching dbt model, return the original spec
            except KeyError:
                pass

        return base_spec
