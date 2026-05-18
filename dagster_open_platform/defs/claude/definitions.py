from dagster import Definitions, EnvVar
from dagster.components import definitions

from dagster_open_platform.defs.claude.assets import (
    anthropic_api_keys,
    anthropic_claude_code_report,
    anthropic_cost_report,
    anthropic_usage_report,
    anthropic_users,
)
from dagster_open_platform.defs.claude.resources import AnthropicAdminResource, claude_resource


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[
            anthropic_usage_report,
            anthropic_cost_report,
            anthropic_claude_code_report,
            anthropic_api_keys,
            anthropic_users,
        ],
        resources={
            "claude": claude_resource,  # Regular Claude API for model inference
            "anthropic_admin": AnthropicAdminResource(
                admin_api_key=EnvVar("ANTHROPIC_ADMIN_API_KEY"),
            ),
        },
    )
