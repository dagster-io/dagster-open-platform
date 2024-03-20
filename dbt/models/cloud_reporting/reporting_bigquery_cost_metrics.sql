{{
  config(
    snowflake_warehouse="L_WAREHOUSE",
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with bigquery_cost_metadata as (
    select *
    from {{ ref('bigquery_cost_metadata') }}
),

base_asset_metrics as (
    select *
    from {{ ref('int_base_asset_metrics') }}
),

reporting_step_data as (
    select *
    from {{ ref('reporting_step_data') }}
),

reporting_bigquery_asset_cost_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'base_asset_metrics.organization_id',
            'base_asset_metrics.deployment_id',
            'base_asset_metrics.step_data_id',
            'base_asset_metrics.asset_key',
            'base_asset_metrics.asset_group',
            'base_asset_metrics.partition',
            'bigquery_cost_metadata.label',
            "'bigquery'",
        ]) }} as unique_key,
        base_asset_metrics.organization_id,
        base_asset_metrics.deployment_id,
        base_asset_metrics.step_data_id,
        base_asset_metrics.asset_key,
        base_asset_metrics.asset_group,
        concat('__cost_bigquery_', bigquery_cost_metadata.label) as metric_name,
        base_asset_metrics.partition,
        sum(bigquery_cost_metadata.bytes_billed) as metric_value,
        max(base_asset_metrics._incremented_at) as last_rebuilt,
        1 as metric_multi_asset_divisor,
        max(run_ended_at) as run_ended_at,
        base_asset_metrics.run_id

    from bigquery_cost_metadata
    inner join base_asset_metrics
        on (
            bigquery_cost_metadata.run_id = base_asset_metrics.run_id
            and bigquery_cost_metadata.step_key = base_asset_metrics.step_key
            and bigquery_cost_metadata.asset_key = base_asset_metrics.asset_key
        )
    where
        bigquery_cost_metadata.bytes_billed is not null
    group by all
),

reporting_bigquery_job_cost_metrics as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'reporting_step_data.organization_id',
            'reporting_step_data.deployment_id',
            'reporting_step_data.id',
            'bigquery_cost_metadata.label',
            "'bigquery'",
        ]) }} as unique_key,
        reporting_step_data.organization_id,
        reporting_step_data.deployment_id,
        reporting_step_data.id,
        null as asset_key,
        null as asset_group,
        concat('__cost_bigquery_', bigquery_cost_metadata.label) as metric_name,
        null as partition,
        sum(bigquery_cost_metadata.bytes_billed) as metric_value,
        max(reporting_step_data.last_rebuilt) as last_rebuilt,
        1 as metric_multi_asset_divisor,
        max(run_ended_at) as run_ended_at,
        reporting_step_data.run_id

    from bigquery_cost_metadata
    inner join reporting_step_data
        on (
            bigquery_cost_metadata.run_id = reporting_step_data.run_id
            and bigquery_cost_metadata.step_key = reporting_step_data.step_key
        )
    where
        bigquery_cost_metadata.bytes_billed is not null
        and bigquery_cost_metadata.asset_key like '["__bigquery_query_metadata_%'
    group by all
)

select * from (
    select * from reporting_bigquery_asset_cost_metrics
    union
    select * from reporting_bigquery_job_cost_metrics
)

{% if is_incremental() %}
where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
{% endif %}
