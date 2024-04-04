{{ config(snowflake_warehouse="L_WAREHOUSE") }}

with runs as (
    select * from {{ ref('fct_runs') }}
),

usage_metrics_daily as (
    select * from {{ ref('usage_metrics_daily') }}
)

select
    organization_id,
    runs.agent_type,
    runs.job_name,
    coalesce(runs.repository_name, '') as repository_name,
    date_trunc('day', runs.ended_at) as job_day,
    {{ dbt_utils.generate_surrogate_key([
        "organization_id", 
        "runs.agent_type", 
        "job_name", 
        "repository_name", 
        "job_day"]) }} as surrogate_key,
    sum(usage_metrics_daily.materializations) as materializations,
    sum(usage_metrics_daily.step_duration_mins) as step_duration_mins,
    sum(usage_metrics_daily.steps) as steps,
    sum(usage_metrics_daily.run_duration_mins) as run_duration_mins,
    sum(usage_metrics_daily.runs) as runs,
    sum(usage_metrics_daily.steps_credits) as steps_credits
from runs
inner join usage_metrics_daily using (organization_id, run_id)
group by all
