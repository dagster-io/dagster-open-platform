import os
from collections.abc import Mapping
from typing import Any

import dagster as dg
from dagster.components import Component, Model, Resolvable, ResolvedAssetSpec
from dagster_dbt import get_asset_key_for_model
from dagster_open_platform.definitions import global_freshness_policy
from dagster_open_platform.defs.dbt.assets import dbt_non_partitioned_models
from dagster_open_platform.defs.hightouch.resources import ConfigurableHightouchResource
from dagster_shared.record import as_dict


def dbt_asset_key(model_name: str) -> dg.AssetKey:
    return get_asset_key_for_model([dbt_non_partitioned_models], model_name)


def spec_with_freshness_policy(spec: dg.AssetSpec) -> dg.AssetSpec:
    return dg.AssetSpec(
        internal_freshness_policy=global_freshness_policy,
        **as_dict(spec),
    )


class DopHightouchSyncComponent(Component, Resolvable, Model):
    asset: ResolvedAssetSpec
    sync_id_env_var: str

    @classmethod
    def get_additional_scope(cls) -> Mapping[str, Any]:
        return {"dbt_asset_key": dbt_asset_key}

    def build_defs(self, context) -> dg.Definitions:
        @dg.multi_asset(name=self.asset.key.path[0], specs=[spec_with_freshness_policy(self.asset)])
        def _assets(hightouch: ConfigurableHightouchResource):
            result = hightouch.sync_and_poll(os.getenv(self.sync_id_env_var, ""))
            return dg.MaterializeResult(
                metadata={
                    "sync_details": result.sync_details,
                    "sync_run_details": result.sync_run_details,
                    "destination_details": result.destination_details,
                    "query_size": result.sync_run_details.get("querySize"),
                    "completion_ratio": result.sync_run_details.get("completionRatio"),
                    "failed_rows": result.sync_run_details.get("failedRows", {}).get("addedCount"),
                }
            )

        return dg.Definitions(assets=[_assets])
