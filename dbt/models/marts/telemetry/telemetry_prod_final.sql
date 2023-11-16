select *


from
    {{ ref('telemetry_prod_daily_run_storage') }}
where
    run_storage_id in (
        select run_storage_id from {{ ref('telemetry_prod_run_storage') }}
    )
order by 2, 1
