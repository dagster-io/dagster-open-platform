select
    'thinkific' as source,
    user_id,
    created_at,
    first_name,
    last_name,
    full_name,
    company,
    email,
    avatar_url,
    affiliate_commission_type,
    headline
from {{ ref('stg_thinkific__users') }}
