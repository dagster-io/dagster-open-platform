{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with base_metrics as (
    select * from {{ ref('base_step_metrics') }}
),

step_data as (
    select * from {{ ref('reporting_step_data') }}
),

pivot as (

    select *
    from base_metrics
    unpivot (
        metric_value for metric_name in (
            dagster_credits, execution_time_ms,
            retry_duration_ms, step_retries,
            step_failures, observations,
            materializations, asset_check_errors,
            asset_check_warnings
        )
    )
),

step_metrics as (
    select

        organization_id,
        deployment_id,
        step_data_id,
        concat('__dagster_', lower(metric_name)) as metric_name,
        metric_value,
        _incremented_at as last_rebuilt,
        run_ended_at

    from pivot
    where
        step_data_id in (
            select id from step_data
        )
        and {{ limit_dates_for_insights(ref_date = 'run_ended_at') }}

        {% if is_incremental() %}
            and run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
        {% endif %}
)

select

    {{ dbt_utils.generate_surrogate_key([
        'step_data_id',
        'metric_name',
    ]) }} as unique_key,
    *
from step_metrics
