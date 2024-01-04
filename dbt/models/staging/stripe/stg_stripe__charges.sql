select

    id as charge_id,
    balance_transaction as balance_transaction_id,
    customer as customer_id,
    invoice as invoice_id,
    payment_intent as payment_intent_id,
    payment_method as payment_method_id,

    amount / 100.0 as amount_dollars,
    amount_captured / 100.0 as amount_captured_dollars,
    amount_refunded / 100.0 as amount_refunded_dollars,
    captured as is_captured,
    currency,
    description,
    disputed as is_disputed,
    failure_code,
    failure_message,
    fraud_details,
    metadata,
    outcome,
    paid as is_paid,
    receipt_email,
    receipt_number,
    receipt_url,
    refunded as is_refunded,
    refunds,
    status,
    created as created_at,
    updated as updated_at,
    updated_by_event_type

from {{ source('stripe', 'charges') }}
