from typing import Any

import dagster as dg
from dagster.components import Component, Model, Resolvable


class ScheduleComponent(Component, Resolvable, Model):
    name: str
    target: str
    tags: dict[str, Any]
    cron: str

    def build_defs(self, context):
        return dg.Definitions(
            schedules=[
                dg.ScheduleDefinition(
                    name=self.name,
                    target=self.target,
                    tags=self.tags,
                    cron_schedule=self.cron,
                )
            ]
        )
