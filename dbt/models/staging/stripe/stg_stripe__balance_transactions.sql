select

    id as balance_transaction_id,
    amount,
    available_on,
    currency,
    description,
    fee / 100.0 as fee_amount_dollars,
    net,
    source,
    status,
    type,
    created as created_at,
    updated as updated_at

from {{ source('stripe', 'balance_transactions') }}
