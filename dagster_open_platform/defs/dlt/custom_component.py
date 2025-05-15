from collections import defaultdict
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster.components.core.context import ComponentLoadContext
from dagster_dlt.components.dlt_load_collection.component import DltLoadCollectionComponent


class CustomDltLoadCollectionComponent(DltLoadCollectionComponent):
    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {
            "daily_not_in_progress": dg.AutomationCondition.cron_tick_passed("0 0 * * *")
            & ~dg.AutomationCondition.in_progress()
        }

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        defs = super().build_defs(context)

        # We explicitly create AssetSpecs for each upstream asset,
        # so we can place them into groups corresponding with the
        # consuming dlt asset
        # Probably not necessary, but needed to get 1:1 snapshots with existing dlt assets
        deps_by_group: dict[str, set[dg.AssetKey]] = defaultdict(set)

        for asset in defs.assets or []:
            if isinstance(asset, dg.AssetsDefinition):
                for asset_key, deps in asset.asset_deps.items():
                    group_name = asset.group_names_by_key[asset_key]
                    deps_by_group[group_name].update(deps)
        specs = []
        for group_name, deps in deps_by_group.items():
            specs.extend([dg.AssetSpec(key=dep, group_name=group_name) for dep in deps])

        return dg.Definitions.merge(defs, dg.Definitions(assets=specs))
