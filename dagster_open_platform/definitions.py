import warnings
from datetime import timedelta

from dagster import Definitions
from dagster._core.definitions.freshness import InternalFreshnessPolicy
from dagster._utils.warnings import BetaWarning, PreviewWarning

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster as dg
import dagster_open_platform.defs

global_freshness_policy_24h = InternalFreshnessPolicy.time_window(fail_window=timedelta(hours=24))


defs = Definitions.merge(dg.components.load_defs(dagster_open_platform.defs))
