from dagster import Definitions
from dagster_open_platform.fivetran.assets import fivetran_assets

defs = Definitions(assets=[*fivetran_assets])
