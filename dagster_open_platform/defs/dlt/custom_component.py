from collections import defaultdict
from collections.abc import Iterator, Mapping
from typing import Any

import dagster as dg
import dagster.components as dg_components
from dagster_dlt import DagsterDltResource, dlt_assets
from dagster_dlt.components.dlt_load_collection.component import DltLoadCollectionComponent


class CustomDltLoadCollectionComponent(DltLoadCollectionComponent):
    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {
            "daily_not_in_progress": dg.AutomationCondition.cron_tick_passed("0 0 * * *")
            & ~dg.AutomationCondition.in_progress()
        }

    ###########################################################################
    # BEGIN TEMP CODE
    # TODO: remove when https://github.com/dagster-io/dagster/pull/30020 is merged
    # Should be removable as of 5/23/25
    ###########################################################################

    @property
    def dlt_pipeline_resource(self) -> "DagsterDltResource":
        from dagster_dlt import DagsterDltResource

        return DagsterDltResource()

    def _temp_build_defs_base(self, context: dg_components.ComponentLoadContext) -> dg.Definitions:
        output = []
        for load in self.loads:

            @dlt_assets(
                dlt_source=load.source,
                dlt_pipeline=load.pipeline,
                name=f"dlt_assets_{load.source.name}_{load.pipeline.dataset_name}",
                dagster_dlt_translator=getattr(load, "translator", None),
            )
            def dlt_assets_def(context: dg.AssetExecutionContext):
                yield from self._temp_execute(context, self.dlt_pipeline_resource)

            output.append(dlt_assets_def)

        return dg.Definitions(assets=output)

    def _temp_execute(
        self, context: dg.AssetExecutionContext, dlt_pipeline_resource: "DagsterDltResource"
    ) -> Iterator:
        """Runs the dlt pipeline. Override this method to customize the execution logic."""
        yield from dlt_pipeline_resource.run(context=context)

    ###########################################################################
    # END TEMP CODE
    ###########################################################################

    def build_defs(self, context: dg_components.ComponentLoadContext) -> dg.Definitions:
        defs = self._temp_build_defs_base(context)

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
