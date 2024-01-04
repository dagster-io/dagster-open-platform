select

    id as customer_id,
    metadata:dagster_cloud_organization_id::number as organization_id,
    name as customer_name,

    account_balance / 100.0 as account_balance_dollars,
    balance / 100.0 as balance_dollars,
    currency,
    default_source,
    delinquent as is_delinquent,
    description,
    email,
    subscriptions,

    created as created_at,
    updated as updated_at

from {{ source('stripe', 'customers') }}
