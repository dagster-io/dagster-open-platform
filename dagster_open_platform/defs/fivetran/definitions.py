from dagster import Definitions
from dagster_open_platform.defs.fivetran.assets import fivetran_assets
from dagster_open_platform.defs.fivetran.checks import account_has_valid_org_id
from dagster_open_platform.defs.snowflake.resources import snowflake_resource
from dagster_open_platform.utils.source_code import add_code_references_and_link_to_git

defs = Definitions(
    assets=add_code_references_and_link_to_git([*fivetran_assets]),
    asset_checks=[account_has_valid_org_id],
    resources={"snowflake_fivetran": snowflake_resource},
)
