select

    id as organization_id,

    name as organization_name,
    public_id,

    internal as is_internal,
    status, -- can be null, what does that mean?

    -- Team_V2 is a new plan type that has capped seats, but for reporting
    -- purposes we want to treat it as a team plan
    case when plan_type = 'TEAM_V2' then 'TEAM' else plan_type end as plan_type,

    parse_json(organization_metadata) as organization_metadata,
    parse_json(organization_settings) as organization_settings,
    not is_null_value(parse_json(organization_metadata):saml_metadata) as has_saml_sso,

    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source("postgres_etl_low_freq", "organizations") }}
