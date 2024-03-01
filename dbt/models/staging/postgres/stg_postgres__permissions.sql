select distinct

    id as permission_id,
    organization_id,
    deployment_id,
    "GRANT" as permission_grant,
    replace(
        json_extract_path_text(
            scope, 'deployment_scope.__enum__'
        ), 'PermissionDeploymentScope.', ''
    ) as scope, -- Pull the scope of the grant out of the json "scope" field.
    create_timestamp as created_at


from {{ source("postgres_etl_low_freq", "permissions") }}
