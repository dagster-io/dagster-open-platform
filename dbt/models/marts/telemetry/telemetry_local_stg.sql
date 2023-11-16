select
    action,
    event_id,
    elapsed_time,
    instance_id,
    metadata,
    python_version,
    dagster_version,
    regexp_substr(dagster_version, $$\d+\.\d+$$) as dag_version_parsed,
    os_platform,
    is_known_ci_env,
    run_storage_id,
    reporting_date,
    metadata:client_id::string as client_id,
    metadata:location_name_hash::string as code_location_hash
from {{ ref('stg_prod_telemetry_oss_telemetry_events_raw') }}
where
    true
