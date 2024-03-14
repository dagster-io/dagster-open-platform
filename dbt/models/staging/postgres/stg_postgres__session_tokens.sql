select

    id as session_token_id,
    organization_id,
    user_id,
    oauth_provider,
    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source("cloud_product", 'session_tokens') }}
