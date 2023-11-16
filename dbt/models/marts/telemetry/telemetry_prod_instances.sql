select instance_id from {{ ref('telemetry_run_instance_mapping') }}
where run_storage_id in (select run_storage_id from {{ ref ('telemetry_prod_run_storage') }})
