select

    organization_id,
    deployment_id,
    metadata_keys

from {{ source("postgres_etl_sling", "reporting_deployment_settings") }}
