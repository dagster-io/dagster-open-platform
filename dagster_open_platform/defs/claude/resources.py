import dagster as dg
from dagster_anthropic import AnthropicResource

claude_resource = AnthropicResource(
    api_key=dg.EnvVar("ANTHROPIC_API_KEY"),
)
