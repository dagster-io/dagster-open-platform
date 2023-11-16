select
    "ACTION" as action,
    event_id,
    elapsed_time,
    instance_id,
    metadata,
    python_version,
    dagster_version,
    os_platform,
    iff(is_known_ci_env = 'True', true, false) as is_known_ci_env,
    run_storage_id,
    try_to_date(client_time) as reporting_date,
    metadata:client_id::string as client_id,
    metadata:location_name_hash::string as code_location_hash
from {{ source('prod_telemetry', 'oss_telemetry_events_raw') }}
where
    try_to_date(client_time) is not null
    and coalesce(try_to_date(client_time), date('1970-01-01')) > '2022-01-01'
    and dagster_version != ''
