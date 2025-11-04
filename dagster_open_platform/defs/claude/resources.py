import dagster as dg
from dagster import ConfigurableResource
from dagster_anthropic import AnthropicResource

# Regular Claude API resource for model inference
claude_resource = AnthropicResource(
    api_key=dg.EnvVar("ANTHROPIC_API_KEY"),
)


# Admin API resource for usage and cost data
class AnthropicAdminResource(ConfigurableResource):
    """Resource for accessing Anthropic Admin API endpoints."""

    admin_api_key: str
