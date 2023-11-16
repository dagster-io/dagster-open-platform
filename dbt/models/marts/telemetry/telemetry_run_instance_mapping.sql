select distinct

    run_storage_id,
    instance_id

from {{ ref('telemetry_prod_raw') }}
where run_storage_id != ''
order by 1
