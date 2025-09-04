-- Test to monitor growth of 'other' workflow type records
-- This test will return rows (indicating failure) when the 'other' workflow type
-- exceeds a reasonable threshold, indicating potential issues with ID changes or logic
--
-- Threshold: More than 5% of total records or more than 100 records in the last 7 days

with recent_workflows as (
    select
        workflow_type,
        count(*) as record_count,
        count(*) * 100.0 / sum(count(*)) over () as percentage_of_total
    from {{ ref('scout_run_logs') }}
    where timestamp_start >= current_date - interval '7 days'
    group by workflow_type
),

other_workflow_metrics as (
    select
        record_count,
        percentage_of_total,
        case 
            when record_count > 100 then 'High volume threshold exceeded (>100 records)'
            when percentage_of_total > 5.0 then 'High percentage threshold exceeded (>5% of total)'
            else null
        end as failure_reason
    from recent_workflows
    where workflow_type = 'other'
)

select
    record_count,
    round(percentage_of_total, 2) as percentage_of_total,
    failure_reason,
    'Other workflow type has grown beyond acceptable thresholds. This may indicate changes in workflow IDs or parsing logic that need investigation.' as alert_message
from other_workflow_metrics
where failure_reason is not null
