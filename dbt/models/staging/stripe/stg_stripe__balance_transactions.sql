select

    id as balance_transaction_id,
    amount,
    available_on,
    currency,
    description,
    fee / 100.0 as fee_amount_dollars,
    net,
    source_id as source,
    status,
    type,
    created as created_at

from {{ source('stripe_pipeline', 'balance_transactions') }}
