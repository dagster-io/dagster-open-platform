-- Delete existing data for the partition date
DELETE FROM raw.anthropic_claude_code
WHERE date IN (
    SELECT DISTINCT date
    FROM raw.temp_anthropic_claude_code
)
