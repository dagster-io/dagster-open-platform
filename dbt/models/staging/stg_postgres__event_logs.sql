{{
    config(
        snowflake_warehouse="L_WAREHOUSE",
        materialized='incremental',
        unique_key='event_log_id',
        cluster_by=["organization_id", "deployment_id", "dagster_event_type", "run_id", "step_key"],
        incremental_strategy='merge',
        incremental_predicates=["DBT_INTERNAL_DEST.created_at > dateadd(day, -7, current_date)"]
    )
}}

with base as (
    select * from {{ source("cloud_product", "event_logs") }}
)

select

    id as event_log_id,

    organization_id,
    deployment_id,
    run_id,

    dagster_event_type,
    asset_key,
    step_key,

    event as event_data,

    timestamp as created_at

from base
where -- noqa: disable=LT02
    {{ limit_dates_for_dev(ref_date = 'timestamp') }}
    and dagster_event_type is not null
    {% if is_incremental() -%}
        and event_log_id > (select max(event_log_id) from {{ this }})
    {% endif %}
