select
    id as teams_users_id,
    user_id,
    team_id,
    organization_id,
    create_timestamp
from {{ source("cloud_product", "teams_users") }}
