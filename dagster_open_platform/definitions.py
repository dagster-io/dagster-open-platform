import warnings

from dagster import Definitions
from dagster._utils.warnings import BetaWarning, PreviewWarning

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster as dg
import dagster_open_platform.defs

defs = Definitions.merge(
    dg.components.load_defs(dagster_open_platform.defs),
)
