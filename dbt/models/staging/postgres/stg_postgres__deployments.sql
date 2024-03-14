select

    id as deployment_id,
    organization_id,

    name as deployment_name,
    subdomain,
    coalesce(status, 'ACTIVE') as status,

    coalesce(status, 'ACTIVE') = 'ACTIVE' as is_active,
    dev_deployment_owner is not null as is_sandbox,
    coalesce(is_branch_deployment::boolean, false) as is_branch_deployment,
    coalesce(agent_type, 'HYBRID') as agent_type,

    coalesce(agent_type, 'HYBRID') = 'HYBRID' as is_hybrid,
    agent_type = 'SERVERLESS' as is_serverless,

    parse_json(deployment_metadata) as deployment_metadata,
    parse_json(deployment_metadata):branch_deployment_metadata:branch_name::string as branch_name,
    parse_json(deployment_metadata):branch_deployment_metadata:branch_url::string as branch_url,
    parse_json(deployment_metadata):branch_deployment_metadata:repo_name::string as repo_name,

    create_timestamp as created_at,
    update_timestamp as updated_at

from {{ source("cloud_product", "deployments") }}
