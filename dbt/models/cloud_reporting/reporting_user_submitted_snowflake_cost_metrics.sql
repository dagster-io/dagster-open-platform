{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}
--Disable sqlfluff rule for unused CTEs, since we
--use them in the incremental case
--noqa: disable=L045

with base_asset_metrics as (
    select *
    from {{ ref('int_base_asset_metrics') }}
    {% if is_incremental() %}
    where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}
),

reporting_step_data as (
    select *
    from {{ ref('reporting_step_data') }}
    {% if is_incremental() %}
    where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}
),

metadata_range_start as (
    select min(run_started_at) as min_date from reporting_step_data
),

metadata_range_end as (
    select max(run_ended_at) as max_date from reporting_step_data
),

snowflake_cost_observation_metadata as (
    select *
    from {{ ref('snowflake_cost_observation_metadata') }}
    {% if is_incremental() %}
    where created_at >= (select min_date from metadata_range_start) and created_at <= (select max_date from metadata_range_end)
    {% endif %}
),

snowflake_cost_submissions as (
    select *
    from {{ ref('stg_insights_snowflake_cost_submissions') }}
),

snowflake_asset_cost_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'base_asset_metrics.organization_id',
            'base_asset_metrics.deployment_id',
            'base_asset_metrics.step_data_id',
            'base_asset_metrics.asset_key',
            'base_asset_metrics.partition',
            'snowflake_cost_submissions.metric_name',
        ]) }} as unique_key,
        base_asset_metrics.organization_id,
        base_asset_metrics.deployment_id,
        base_asset_metrics.step_data_id,
        base_asset_metrics.asset_key,
        base_asset_metrics.asset_group,
        concat('__cost_', snowflake_cost_submissions.metric_name) as metric_name,
        base_asset_metrics.partition,
        sum(snowflake_cost_submissions.snowflake_cost) as metric_value,
        max(base_asset_metrics._incremented_at) as last_rebuilt,
        1 as metric_multi_asset_divisor,
        max(run_ended_at) as run_ended_at


    from snowflake_cost_submissions
    inner join snowflake_cost_observation_metadata
        on snowflake_cost_submissions.opaque_id = snowflake_cost_observation_metadata.opaque_id
    inner join base_asset_metrics
        on (
            snowflake_cost_observation_metadata.run_id = base_asset_metrics.run_id
            and snowflake_cost_observation_metadata.step_key = base_asset_metrics.step_key
            and snowflake_cost_observation_metadata.asset_key = base_asset_metrics.asset_key
        )
    where
        snowflake_cost_submissions.opaque_id is not null
    group by all
),

snowflake_job_cost_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'reporting_step_data.organization_id',
            'reporting_step_data.deployment_id',
            'reporting_step_data.id',
            'snowflake_cost_submissions.metric_name',
        ]) }} as unique_key,
        reporting_step_data.organization_id,
        reporting_step_data.deployment_id,
        reporting_step_data.id,
        null as asset_key,
        null as asset_group,
        concat('__cost_', snowflake_cost_submissions.metric_name) as metric_name,
        null as partition,
        sum(snowflake_cost_submissions.snowflake_cost) as metric_value,
        max(reporting_step_data.last_rebuilt) as last_rebuilt,
        1 as metric_multi_asset_divisor,
        max(run_ended_at) as run_ended_at


    from snowflake_cost_submissions
    inner join snowflake_cost_observation_metadata
        on snowflake_cost_submissions.opaque_id = snowflake_cost_observation_metadata.opaque_id
    inner join reporting_step_data
        on (
            snowflake_cost_observation_metadata.run_id = reporting_step_data.run_id
            and snowflake_cost_observation_metadata.step_key = reporting_step_data.step_key
        )
    where
        snowflake_cost_submissions.opaque_id is not null
        and snowflake_cost_observation_metadata.asset_key like '["__snowflake_query_metadata_%'
    group by all
)

select * from (
    select * from snowflake_asset_cost_metrics
    union
    select * from snowflake_job_cost_metrics
)

{% if is_incremental() %}
where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
{% endif %}
