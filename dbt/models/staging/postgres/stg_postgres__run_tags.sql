select

    id as run_tag_id,
    run_id,
    organization_id,
    deployment_id,

    key,
    value

from {{ source('cloud_product', 'run_tags') }}
