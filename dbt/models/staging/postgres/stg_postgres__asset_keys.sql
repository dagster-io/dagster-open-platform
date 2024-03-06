select

    id as asset_key_id,

    deployment_id
        as last_run_id,
    organization_id,

    to_array(parse_json(asset_key)) as asset_key,
    parse_json(asset_details) as asset_details,
    parse_json(cached_status_data) as cached_status_data,
    parse_json(last_materialization) as last_materialization,

    create_timestamp as created_at,
    last_materialization_timestamp as last_materialized_at,
    wipe_timestamp as wiped_at


from {{ source("postgres_etl_low_freq", "asset_keys") }}
