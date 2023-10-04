select

    id as permission_id,
    organization_id,
    deployment_id,
    "GRANT" as permission_grant,

    create_timestamp as created_at


from {{ source("postgres_etl_low_freq", "permissions") }}
