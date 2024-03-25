{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with deduped_internal_asset_materialization_events as (
    select * from {{ ref('reporting_deduped_internal_asset_materialization_metrics') }}
),

metadata_asset_materialization_events as (
    select * from {{ ref('reporting_metadata_asset_materialization_metrics') }}
),

reporting_user_submitted_snowflake_cost_metrics as (
    select * from {{ ref('reporting_user_submitted_snowflake_cost_metrics') }}
    where asset_key is not null
),

reporting_bigquery_cost_metrics as (
    select * from {{ ref('reporting_bigquery_cost_metrics') }}
    where asset_key is not null
)

(
    select * from deduped_internal_asset_materialization_events
    {% if is_incremental() %}
        where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}

)
union
(
    select * from metadata_asset_materialization_events
    {% if is_incremental() %}
        where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}

)
union
(
    select * from reporting_user_submitted_snowflake_cost_metrics
    {% if is_incremental() %}
        where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}
)
union
(
    select * from reporting_bigquery_cost_metrics
    {% if is_incremental() %}
        where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
    {% endif %}
)
