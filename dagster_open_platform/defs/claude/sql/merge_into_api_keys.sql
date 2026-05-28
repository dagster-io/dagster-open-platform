INSERT INTO raw.anthropic_api_keys (
    id,
    name,
    workspace_id,
    created_at,
    created_by_id,
    created_by_type,
    status,
    expires_at,
    partial_key_hint,
    extracted_at
)
SELECT
    id,
    name,
    workspace_id,
    created_at,
    created_by_id,
    created_by_type,
    status,
    TRY_TO_TIMESTAMP_NTZ(TO_VARCHAR(expires_at)) AS expires_at,
    partial_key_hint,
    extracted_at
FROM raw.temp_anthropic_api_keys
