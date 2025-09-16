import warnings
from datetime import timedelta

from dagster._core.definitions.freshness import InternalFreshnessPolicy
from dagster._utils.warnings import BetaWarning, PreviewWarning
from dagster_open_platform.utils.source_code import link_defs_code_references_to_git

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster as dg

global_freshness_policy = InternalFreshnessPolicy.time_window(fail_window=timedelta(hours=36))


@dg.components.definitions
def defs() -> dg.Definitions:
    import dagster_open_platform.defs

    return link_defs_code_references_to_git(dg.components.load_defs(dagster_open_platform.defs))
