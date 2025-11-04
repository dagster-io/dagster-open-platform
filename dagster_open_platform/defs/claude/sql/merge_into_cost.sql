-- Insert all new data for the partition
INSERT INTO raw.anthropic_costs (
    currency,
    amount,
    workspace_id,
    description,
    cost_type,
    context_window,
    model,
    service_tier,
    token_type,
    starting_at,
    ending_at,
    extracted_at
)
SELECT
    currency,
    amount,
    workspace_id,
    description,
    cost_type,
    context_window,
    model,
    service_tier,
    token_type,
    starting_at,
    ending_at,
    extracted_at
FROM raw.temp_anthropic_costs