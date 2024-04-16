select
    id as repository_location_id,
    organization_id,
    deployment_id,
    asset_keys,
    group_names,
    parse_json(dagster_library_versions) as dagster_library_versions,
    create_timestamp as created_at,
    update_timestamp as updated_at 
from {{ source("cloud_product", "repository_locations_data") }}
