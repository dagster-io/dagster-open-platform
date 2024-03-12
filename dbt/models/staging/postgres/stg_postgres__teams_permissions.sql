select
    id as teams_permissions_id,
    permission_id,
    team_id,
    organization_id,
    create_timestamp
from {{ source("postgres_etl_low_freq", "teams_permissions") }}
