import dagster as dg
from dagster_dlt import DagsterDltResource

defs = dg.Definitions(
    resources={
        "dlt": DagsterDltResource(),
    },
)
