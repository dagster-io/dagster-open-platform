import dagster as dg
from dagster_open_platform.definitions import global_freshness_policy


class SegmentComponent(dg.components.Component, dg.components.Model, dg.components.Resolvable):
    keys: dict[str, dict[str, list[str]]]
    group_name: str
    description: str
    kinds: list[str]

    def build_defs(self, context: dg.components.ComponentLoadContext) -> dg.Definitions:
        asset_keys = []
        for source in self.keys:
            for category in self.keys[source]:
                for event_type in self.keys[source][category]:
                    asset_keys.append([source, category, event_type])

        return dg.Definitions(
            assets=[
                dg.AssetSpec(
                    key=key,
                    group_name=self.group_name,
                    description=self.description,
                    internal_freshness_policy=global_freshness_policy,
                    kinds=set(self.kinds),
                )
                for key in asset_keys
            ]
        )
