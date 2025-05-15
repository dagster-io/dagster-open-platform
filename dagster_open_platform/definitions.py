import warnings
from datetime import timedelta

from dagster._core.definitions.freshness import InternalFreshnessPolicy
from dagster._utils.warnings import BetaWarning, PreviewWarning

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster as dg

global_freshness_policy = InternalFreshnessPolicy.time_window(fail_window=timedelta(hours=23))


@dg.components.definitions
def defs() -> dg.Definitions:
    import dagster_open_platform.defs

    return dg.components.load_defs(dagster_open_platform.defs)
