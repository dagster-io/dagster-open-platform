import os

from dagster_anthropic import AnthropicResource

claude_resource = AnthropicResource(
    api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
)
