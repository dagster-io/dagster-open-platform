from dagster import Definitions, EnvVar
from dagster.components import definitions

from .assets import anthropic_cost_report, anthropic_usage_report
from .resources import AnthropicAdminResource, claude_resource


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[anthropic_usage_report, anthropic_cost_report],
        resources={
            "claude": claude_resource,  # Regular Claude API for model inference
            "anthropic_admin": AnthropicAdminResource(
                admin_api_key=EnvVar("ANTHROPIC_ADMIN_API_KEY"),
            ),
        },
    )
