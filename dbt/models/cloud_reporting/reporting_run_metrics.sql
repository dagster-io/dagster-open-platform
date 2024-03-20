{{
  config(
    snowflake_warehouse="L_WAREHOUSE",
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with step_metrics as (
    select * from {{ ref('reporting_step_metrics') }}
),

step_data as (
    select * from {{ ref('reporting_step_data') }}
),

reporting_bigquery_cost_metrics as (
    select * from {{ ref('reporting_bigquery_cost_metrics') }}
),

reporting_user_submitted_snowflake_cost_metrics as (
    select * from {{ ref('reporting_user_submitted_snowflake_cost_metrics') }}
),

aggregated_step_metrics as (
    select
        rsd.organization_id,
        rsd.deployment_id,
        rsd.repository_name,
        rsd.code_location_name,
        rsd.pipeline_name,
        rsd.run_id,
        rsm.metric_name,
        date_trunc('day', rsd.step_end_timestamp) as rollup_date,
        to_number(sum(rsm.metric_value), 38, 12) as metric_value_sum,
        max(rsm.last_rebuilt) as last_rebuilt,
        max(rsd.run_ended_at) as run_ended_at
    from
        step_metrics as rsm
    left join step_data as rsd
        on rsm.step_data_id = rsd.id

    where
        date_trunc('day', rsd.step_end_timestamp) > '2023-06-01'
        and rsd.run_ended_at is not null

    {% if is_incremental() %}
        and rsd.run_ended_at  >= '{{ var('min_date') }}' and rsd.run_ended_at  < '{{ var('max_date') }}'
    {% endif %}

    group by
        1, 2, 3, 4, 5, 6, 7, 8
),

aggregated_snowflake_cost_metrics as (
    select
        rsd.organization_id,
        rsd.deployment_id,
        rsd.repository_name,
        rsd.code_location_name,
        rsd.pipeline_name,
        rsd.run_id,
        usscm.metric_name,
        date_trunc('day', rsd.step_end_timestamp) as rollup_date,
        to_number(sum(usscm.metric_value), 38, 12) as metric_value_sum,
        max(usscm.last_rebuilt) as last_rebuilt,
        max(rsd.run_ended_at) as run_ended_at
    from
        reporting_user_submitted_snowflake_cost_metrics as usscm
    left join step_data as rsd
        on usscm.step_data_id = rsd.id

    where
        date_trunc('day', rsd.step_end_timestamp) > '2023-06-01'
        and rsd.run_ended_at is not null

    {% if is_incremental() %}
        and rsd.run_ended_at  >= '{{ var('min_date') }}' and rsd.run_ended_at  < '{{ var('max_date') }}'
    {% endif %}

    group by
        1, 2, 3, 4, 5, 6, 7, 8
),

aggregated_bigquery_cost_metrics as (
    select
        rsd.organization_id,
        rsd.deployment_id,
        rsd.repository_name,
        rsd.code_location_name,
        rsd.pipeline_name,
        rsd.run_id,
        bqcm.metric_name,
        date_trunc('day', rsd.step_end_timestamp) as rollup_date,
        to_number(sum(bqcm.metric_value), 38, 12) as metric_value_sum,
        max(bqcm.last_rebuilt) as last_rebuilt,
        max(rsd.run_ended_at) as run_ended_at
    from
        reporting_bigquery_cost_metrics as bqcm
    left join step_data as rsd
        on bqcm.step_data_id = rsd.id

    where
        date_trunc('day', rsd.step_end_timestamp) > '2023-06-01'
        and rsd.run_ended_at is not null

    {% if is_incremental() %}
        and rsd.run_ended_at  >= '{{ var('min_date') }}' and rsd.run_ended_at  < '{{ var('max_date') }}'
    {% endif %}

    group by
        1, 2, 3, 4, 5, 6, 7, 8
),

run_duration_metrics as (
    select
        runs.organization_id,
        runs.deployment_id,
        runs.repository_name,
        runs.code_location_name,
        runs.job_name as pipeline_name,
        runs.run_id,
        '__dagster_run_duration_ms' as metric_name,
        date_trunc('day', runs.ended_at) as rollup_date,
        to_number(runs.duration_ms, 38, 12) as metric_value_sum,
        to_timestamp_ltz(runs.ended_at) as last_rebuilt,
        runs.ended_at as run_ended_at
    from {{ ref('fct_runs') }} as runs

    where
        date_trunc('day', runs.ended_at) > '2023-06-01'
        and runs.ended_at is not null

    {% if is_incremental() %}
        and runs.ended_at >= '{{ var('min_date') }}' and runs.ended_at < '{{ var('max_date') }}'
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'organization_id',
        'deployment_id',
        'repository_name',
        'code_location_name',
        'pipeline_name',
        'run_id',
        'metric_name',
        'rollup_date'
    ]) }} as unique_key,
    *
from (
    select * from aggregated_step_metrics
    union
    select * from run_duration_metrics
    union
    select * from aggregated_snowflake_cost_metrics
    union
    select * from aggregated_bigquery_cost_metrics
)
