INSERT INTO raw.anthropic_users (
    id,
    email,
    name,
    role,
    added_at,
    extracted_at
)
SELECT
    id,
    email,
    name,
    role,
    added_at,
    extracted_at
FROM raw.temp_anthropic_users
