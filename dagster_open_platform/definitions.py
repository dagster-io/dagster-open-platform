import warnings
from datetime import timedelta

from dagster import AssetSelection, FreshnessPolicy, apply_freshness_policy
from dagster._utils.warnings import BetaWarning, PreviewWarning
from dagster_open_platform.utils.source_code import link_defs_code_references_to_git

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster as dg

global_freshness_policy = FreshnessPolicy.time_window(fail_window=timedelta(hours=36))


@dg.components.definitions
def defs() -> dg.Definitions:
    import dagster_open_platform.defs

    defs = dg.components.load_defs(dagster_open_platform.defs)

    defs = defs.permissive_map_resolved_asset_specs(
        func=lambda spec: apply_freshness_policy(spec, global_freshness_policy),
        selection=AssetSelection.all().materializable(),
    )

    return link_defs_code_references_to_git(defs)
