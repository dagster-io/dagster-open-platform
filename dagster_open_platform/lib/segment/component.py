import dagster as dg


class SegmentComponent(dg.components.Component, dg.components.Model, dg.components.Resolvable):
    keys: dict[str, dict[str, list[str]]]
    group_name: str
    description: str
    kinds: list[str]

    def build_defs(self, context: dg.components.ComponentLoadContext) -> dg.Definitions:
        asset_keys = []
        for source in self.keys:
            for category in self.keys[source]:
                asset_keys.extend(
                    [source, category, event_type] for event_type in self.keys[source][category]
                )

        return dg.Definitions(
            assets=[
                dg.AssetSpec(
                    key=key,
                    group_name=self.group_name,
                    description=self.description,
                    kinds=set(self.kinds),
                )
                for key in asset_keys
            ]
        )
