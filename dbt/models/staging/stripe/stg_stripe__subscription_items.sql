select

    id as subscription_item_id,
    batch_timestamp,
    billing_thresholds_usage_gte,
    created as created_at,
    merchant_id,

    plan_amount,
    plan_created,
    plan_currency,
    plan_id,
    plan_interval,
    plan_interval_count,
    plan_nickname,
    plan_product_id,
    plan_trial_period_days,

    price_created,
    price_currency,
    price_id,
    price_nickname,
    price_product_id,
    price_recurring_interval,
    price_recurring_interval_count,
    price_recurring_trial_period_days,
    price_unit_amount,

    quantity,

    subscription_id

from {{ source('stripe_pipeline', 'subscription_items') }}
