select

    id as line_item_id,
    invoice as invoice_id,
    subscription as subscription_id,
    subscription_item as subscription_item_id,
    invoice_item as invoice_item_id,

    metadata:dagster_cloud_organization_id::number as organization_id,
    metadata:dagster_cloud_organization_name::string as organization_name,
    metadata:managed_by::string as managed_by,
    metadata:plan as metadata_plan,

    type as line_item_type,
    amount / 100.0 as line_item_amount_dollars,
    currency,
    description as line_item_description,
    discountable as is_discountable,
    discounts,
    discount_amounts,
    quantity,

    period:start::timestamp as period_start,
    period:end::timestamp as period_end,
    plan,
    price,
    proration,
    proration_details

from {{ source('stripe', 'invoice_line_items') }}
