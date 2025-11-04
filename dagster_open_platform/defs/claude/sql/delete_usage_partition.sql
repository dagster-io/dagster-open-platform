-- Delete existing data for the partition time window
DELETE FROM raw.anthropic_usage
WHERE (starting_at, ending_at) IN (
    SELECT DISTINCT starting_at, ending_at
    FROM raw.temp_anthropic_usage
)

