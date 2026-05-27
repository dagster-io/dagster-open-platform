import dagster as dg
from dagster.components import definitions
from dagster_open_platform.defs.common_room.assets import common_room_signals


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        assets=[common_room_signals],
    )
