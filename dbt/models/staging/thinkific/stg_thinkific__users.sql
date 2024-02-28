select
    id,
    created_at,
    first_name,
    last_name,
    full_name,
    company,
    email,
    avatar_url,
    affiliate_commission_type,
    headline
from {{ source('thinkific', 'users') }}
