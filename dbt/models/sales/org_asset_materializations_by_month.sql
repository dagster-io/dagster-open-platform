select

    date_trunc('month', created_at) as month,
    organization_id,
    count(
        {{ dbt_utils.generate_surrogate_key(["asset_key", "run_id", "deployment_id"]) }}
    ) as count_assets

from {{ ref('stg_postgres__event_logs') }}
where dagster_event_type = 'ASSET_MATERIALIZATION'
{% if is_incremental() %}
and created_at >= '{{ var('min_date') }}' AND created_at <= '{{ var('max_date') }}'
{% endif %}
group by 1, 2
