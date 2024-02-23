select

    organization_id,
    deployment_id,
    opaque_id,
    metric_name,
    query_id,
    max_by(snowflake_cost, last_updated) as snowflake_cost,
    max(last_updated) as last_updated

from (
    -- Add up costs within an individual submission file
    select
        organization_id,
        deployment_id,
        opaque_id,
        metric_name,
        query_id,
        submission_file_name,
        sum(cost) as snowflake_cost,
        max(last_updated) as last_updated

    from {{ source('purina_staging', 'insights_metrics_submissions') }}

    group by 1, 2, 3, 4, 5, 6
)

group by 1, 2, 3, 4, 5
