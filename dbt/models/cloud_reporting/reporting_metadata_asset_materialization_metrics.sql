{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
  )
}}

with metadata_metrics as (
    select * from {{ ref('int_base_metadata_asset_metrics') }}
),

step_data as (
    select * from {{ ref('reporting_step_data') }}
)

select

    {{ dbt_utils.generate_surrogate_key([
        'mm.organization_id',
        'mm.deployment_id',
        'mm.step_data_id',
        'mm.asset_key',
        'mm.asset_group',
        'mm.partition',
        'mm.label',
    ]) }} as unique_key,
    mm.organization_id,
    mm.deployment_id,
    mm.step_data_id,
    mm.asset_key,
    mm.asset_group,
    concat('__meta_', mm.label) as metric_name,
    mm.partition,
    sum(mm.metadata_value) as metric_value,
    max(mm._incremented_at) as last_rebuilt,
    1 as metric_multi_asset_divisor,
    max(mm.run_ended_at) as run_ended_at,
    step_data.run_id

from metadata_metrics as mm
left join step_data on mm.step_data_id = step_data.id

{% if is_incremental() %}
    where mm.run_ended_at >= '{{ var('min_date') }}' and mm.run_ended_at < '{{ var('max_date') }}'
{% endif %}

group by all
