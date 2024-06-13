from dagster import Definitions
from dagster_open_platform.fivetran.assets import fivetran_assets

from ..utils.source_code import add_code_references_and_link_to_git

defs = Definitions(assets=add_code_references_and_link_to_git([*fivetran_assets]))
