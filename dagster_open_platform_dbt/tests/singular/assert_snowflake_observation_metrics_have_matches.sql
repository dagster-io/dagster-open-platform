{{ config(
    meta = {
        'dagster': {
            'ref': {
                'name': 'reporting_unmatched_snowflake_cost_observation_metadata'
            }
        }
    }
) }}

with num_unmatched_observations as (
    select
        count(1) as num_unmatched,
        1 as id
    from {{ ref('reporting_unmatched_snowflake_cost_observation_metadata') }} as unmatched_metrics
    where unmatched_metrics.created_at > current_date - 14 -- only look at the last 14 days
),

num_matched_observations as (
    select
        count(1) as num_matched,
        1 as id
    from {{ ref('reporting_user_submitted_snowflake_cost_metrics') }} as matched_metrics
    where matched_metrics.run_ended_at > current_date - 14 -- only look at the last 14 days
),

unmatched_stats as (
    select coalesce(num_unmatched_observations.num_unmatched / NULLIF(num_matched_observations.num_matched + num_unmatched_observations.num_unmatched, 0), 0) * 100 as unmatched_percentage
    from num_unmatched_observations
    inner join num_matched_observations
    on num_unmatched_observations.id = num_matched_observations.id

)

select * from unmatched_stats where unmatched_percentage > 50
