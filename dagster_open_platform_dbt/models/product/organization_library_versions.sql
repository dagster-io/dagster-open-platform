with

repo_locations_data as (
    select *
    from {{ ref("stg_postgres__repository_locations_data") }}
),

organizations as (
    select
        organization_id,
        organization_name
    from {{ ref('stg_postgres__organizations') }}
)

select
    organization_id,
    organization_name,
    key as library,
    max(replace(value, '"')) as version
from 
    repo_locations_data
    join organizations using (organization_id),
    lateral flatten(input => repo_locations_data.dagster_library_versions)
group by all