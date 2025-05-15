import dagster as dg
from dagster.components import definitions
from dagster_dlt import DagsterDltResource


@definitions
def defs() -> dg.Definitions:
    return dg.Definitions(
        resources={
            "dlt": DagsterDltResource(),
        },
    )
