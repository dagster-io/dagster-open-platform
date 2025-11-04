from dagster import Definitions, EnvVar
from dagster.components import definitions

from .assets import anthropic_cost_report, anthropic_usage_report
from .resources import AnthropicAdminResource, claude_resource
from .schedules import anthropic_cost_daily_schedule, anthropic_usage_daily_schedule


@definitions
def defs() -> Definitions:
    return Definitions(
        assets=[anthropic_usage_report, anthropic_cost_report],
        schedules=[anthropic_usage_daily_schedule, anthropic_cost_daily_schedule],
        resources={
            "claude": claude_resource,  # Regular Claude API for model inference
            "anthropic_admin": AnthropicAdminResource(
                admin_api_key=EnvVar("ANTHROPIC_ADMIN_API_KEY"),
            ),
        },
    )
