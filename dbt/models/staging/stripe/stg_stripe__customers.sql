select

    id as customer_id,
    name as customer_name,

    account_balance / 100.0 as account_balance_dollars,
    currency,
    delinquent as is_delinquent,
    description,
    email,

    created as created_at

from {{ source('stripe_pipeline', 'customers') }}
