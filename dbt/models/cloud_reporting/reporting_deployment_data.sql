{{ 
    config(
        tags=["insights"]
    )
}}

with latest_data as (
    select
        organization_id,
        deployment_id,
        max(end_time) as latest_data_timestamp
    from {{ ref('fct_steps' ) }}
    group by organization_id, deployment_id
),

deployment_steps_metric_types as (
    select
        organization_id,
        deployment_id,
        array_unique_agg(metric_name) as metric_types_array
    from {{ ref('reporting_metric_types') }}
    where
        source = 'step_metrics'
    group by organization_id, deployment_id
),

materialization_metadata_labels as (
    select
        organization_id,
        deployment_id,
        label,
        max(created_at) as latest_created_at,
        count(*) as num_materializations,
        count(distinct asset_key) as num_assets
    from {{ ref('materialization_metadata') }}
    where created_at > dateadd(day, -60, current_date)
    group by organization_id, deployment_id, label
),

deployment_asset_metadata_types as (
    select
        organization_id,
        deployment_id,
        array_agg(object_construct(
            'label', label,
            'latest_created_at', latest_created_at,
            'num_materializations', num_materializations,
            'num_assets', num_assets
        )) as metadata_labels_array
    from materialization_metadata_labels
    group by organization_id, deployment_id
),

deployment_assets_metric_types as (
    select
        organization_id,
        deployment_id,
        array_unique_agg(metric_name) as metric_types_array
    from {{ ref('reporting_metric_types') }}
    where
        source = 'asset_materialization_metrics'
    group by organization_id, deployment_id
),


deployments as (
    select * from {{ ref('stg_postgres__deployments') }}
)

select distinct

    deployments.organization_id,
    deployments.deployment_id,

    1 as downsampling_rate,
    latest_data.latest_data_timestamp,
    to_json(
        object_construct(
            'assets_metric_types',
            deployment_assets_metric_types.metric_types_array,
            'steps_metric_types',
            deployment_steps_metric_types.metric_types_array,
            'asset_metadata_types',
            deployment_asset_metadata_types.metadata_labels_array
        )
    ) as deployment_metadata

from
    deployments
left join
    latest_data
    on
        deployments.organization_id = latest_data.organization_id
        and deployments.deployment_id = latest_data.deployment_id
left join
    deployment_assets_metric_types
    on
        deployments.organization_id
        = deployment_assets_metric_types.organization_id
        and deployments.deployment_id
        = deployment_assets_metric_types.deployment_id
left join
    deployment_steps_metric_types
    on
        deployments.organization_id
        = deployment_steps_metric_types.organization_id
        and deployments.deployment_id
        = deployment_steps_metric_types.deployment_id
left join
    deployment_asset_metadata_types
    on
        deployments.organization_id
        = deployment_asset_metadata_types.organization_id
        and deployments.deployment_id
        = deployment_asset_metadata_types.deployment_id
