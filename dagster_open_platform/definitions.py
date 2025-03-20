import warnings

from dagster._utils.warnings import BetaWarning, PreviewWarning

warnings.filterwarnings("ignore", category=PreviewWarning)
warnings.filterwarnings("ignore", category=BetaWarning)

import dagster_open_platform.aws.definitions as aws_definitions
import dagster_open_platform.dbt.definitions as dbt_definitions
import dagster_open_platform.dlt.definitions as dlt_definitions
import dagster_open_platform.fivetran.definitions as fivetran_definitions
import dagster_open_platform.gong.definitions as gong_definitions
import dagster_open_platform.hightouch.definitions as hightouch_definitions
import dagster_open_platform.pypi.definitions as pypi_definitions
import dagster_open_platform.quickstart.definitions as quickstart_definitions
import dagster_open_platform.scout.definitions as scout_definitions
import dagster_open_platform.segment.definitions as segment_definitions
import dagster_open_platform.slack.definitions as slack_definitions
import dagster_open_platform.sling.definitions as sling_definitions
import dagster_open_platform.sling_custom.definitions as sling_custom_definitions
import dagster_open_platform.snowflake.definitions as snowflake_definitions
import dagster_open_platform.stripe.definitions as stripe_definitions
from dagster import Definitions

defs = Definitions.merge(
    aws_definitions.defs,
    dbt_definitions.defs,
    dlt_definitions.defs,
    fivetran_definitions.defs,
    gong_definitions.defs,
    hightouch_definitions.defs,
    pypi_definitions.defs,
    quickstart_definitions.defs,
    scout_definitions.defs,
    segment_definitions.defs,
    slack_definitions.defs,
    sling_custom_definitions.defs,
    sling_definitions.defs,
    snowflake_definitions.defs,
    stripe_definitions.defs,
)
