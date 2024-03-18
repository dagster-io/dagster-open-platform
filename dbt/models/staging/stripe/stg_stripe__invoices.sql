select

    id as invoice_id,

    charge_id,
    customer_id,
    subscription_id,

    amount_due / 100.0 as amount_due_dollars,
    amount_paid / 100.0 as amount_paid_dollars,
    amount_remaining / 100.0 as amount_remaining_dollars,
    ending_balance / 100.0 as ending_balance_dollars,
    subtotal / 100.0 as invoice_subtotal_dollars,
    tax / 100.0 as invoice_tax_dollars,
    total / 100.0 as invoice_total_dollars,

    customer_name,
    customer_email,
    attempt_count,
    -- billing as billing_type,
    billing_reason,
    collection_method,
    currency,
    description as invoice_memo,
    -- discount,
    due_date as due_at,
    number,
    -- closed as is_closed,
    paid as is_paid,
    period_start,
    period_end,
    status,

    date as invoice_created_at
    -- coalesce(status_transitions:finalized_at::timestamp, finalized_at) as finalized_at


from {{ source('stripe_pipeline', 'invoices') }}
