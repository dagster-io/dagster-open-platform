select

    id as customer_info_id,
    organization_id,
    stripe_customer_id,
    plan_type,
    status,
    parse_json(serialized) as customer_metadata,
    create_timestamp as created_at

from {{ source("postgres_etl_low_freq", "customer_info") }}
