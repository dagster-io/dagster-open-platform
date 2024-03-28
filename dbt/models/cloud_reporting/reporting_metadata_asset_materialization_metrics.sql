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
)

select

    {{ dbt_utils.generate_surrogate_key([
        'organization_id',
        'deployment_id',
        'step_data_id',
        'asset_key',
        'asset_group',
        'partition',
        'label',
    ]) }} as unique_key,
    organization_id,
    deployment_id,
    step_data_id,
    asset_key,
    asset_group,
    concat('__meta_', label) as metric_name,
    partition,
    sum(metadata_value) as metric_value,
    max(_incremented_at) as last_rebuilt,
    1 as metric_multi_asset_divisor,
    max(run_ended_at) as run_ended_at

from metadata_metrics


{% if is_incremental() %}
    where run_ended_at >= '{{ var('min_date') }}' and run_ended_at < '{{ var('max_date') }}'
{% endif %}

group by 1, 2, 3, 4, 5, 6, 7, 8
