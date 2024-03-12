select
    id as team_id,
    name,
    organization_id,
    creator_id,
    scim_external_id,
    metadata,
    create_timestamp,
    update_timestamp
from {{ source("postgres_etl_low_freq", "teams") }}
