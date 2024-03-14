select

    id as user_id,
    email,
    name as user_name,
    title as user_title,
    experience,
    email like '%@elementl%' or email like '%@dagster.%' as is_elementl_user,
    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source("cloud_product", 'users') }}
