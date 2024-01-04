select
    subscription_id,
    key,
    value

from {{ source('stripe_pipeline', 'subscriptions_metadata') }}
