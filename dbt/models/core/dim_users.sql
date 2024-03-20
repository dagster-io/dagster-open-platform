select

    user_id,
    email,
    user_name,
    user_title,
    experience,
    created_at,
    updated_at

from {{ ref('stg_postgres__users') }}
where not is_elementl_user
