-- Insert all new data for the partition
INSERT INTO raw.anthropic_usage (
    uncached_input_tokens,
    cache_creation_ephemeral_1h_input_tokens,
    cache_creation_ephemeral_5m_input_tokens,
    cache_read_input_tokens,
    output_tokens,
    web_search_requests,
    api_key_id,
    workspace_id,
    model,
    service_tier,
    context_window,
    starting_at,
    ending_at,
    extracted_at
)
SELECT
    uncached_input_tokens,
    cache_creation_ephemeral_1h_input_tokens,
    cache_creation_ephemeral_5m_input_tokens,
    cache_read_input_tokens,
    output_tokens,
    web_search_requests,
    api_key_id,
    workspace_id,
    model,
    service_tier,
    context_window,
    starting_at,
    ending_at,
    extracted_at
FROM raw.temp_anthropic_usage