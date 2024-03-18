select

    id as charge_id,
    balance_transaction_id,
    customer_id,
    invoice_id,
    payment_intent as payment_intent_id,
    payment_method_id,

    amount / 100.0 as amount_dollars,
    iff(captured, amount_dollars, 0) as amount_captured_dollars,
    amount_refunded / 100.0 as amount_refunded_dollars,
    captured as is_captured,
    currency,
    description,
    dispute_id is not null as is_disputed,
    failure_code,
    failure_message,
    paid as is_paid,
    receipt_email,
    receipt_number,
    refunded as is_refunded,
    status,
    created as created_at

from {{ source('stripe_pipeline', 'charges') }}
