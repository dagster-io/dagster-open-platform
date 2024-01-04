select

    id as plan_id,
    name as plan_name,

    active as is_active,
    amount_decimal / 100.0 as plan_amount_dollars,
    billing_scheme,
    currency,
    interval,
    interval_count,
    metadata,
    nickname,
    product as product_id,
    tiers,
    usage_type,
    updated_by_event_type,
    created as created_at,
    updated as updated_at

from {{ source('stripe', 'plans') }}
