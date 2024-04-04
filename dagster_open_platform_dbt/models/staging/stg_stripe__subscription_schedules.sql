select

    id as schedule_id,
    subscription as subscription_id,
    customer as customer_id,
    canceled_at,
    completed_at,
    created as created_at,
    end_behavior,
    released_at,
    released_subscription

from {{ source('stripe_pipeline', 'subscription_schedules') }}
