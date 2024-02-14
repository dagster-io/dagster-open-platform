select

    organization_id,
    deployment_id,
    opaque_id,
    metric_name,
    query_id,
    max_by(cost, last_updated) as snowflake_cost,
    max(last_updated) as last_updated

from {{ source('purina_staging', 'insights_metrics_submissions') }}

group by 1, 2, 3, 4, 5
