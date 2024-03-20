select

    id as plan_id,

    amount / 100.0 as plan_amount_dollars,
    billing_scheme,
    currency,
    interval,
    interval_count,
    nickname,
    product_id,
    tiers_mode,
    usage_type,
    created as created_at

from {{ source('stripe_pipeline', 'plans') }}
