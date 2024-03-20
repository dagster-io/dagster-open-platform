with deployments as (

    select * from {{ ref('stg_postgres__deployments') }}

),

final as (

    select

        organization_id,
        date_trunc('day', created_at) as deployment_created_day,
        count(deployment_id) as total_deployments,
        count_if(is_active and not is_sandbox and not is_branch_deployment)
            as total_active_deployments,
        count_if(is_branch_deployment) as total_branch_deployments,
        count_if(is_hybrid) as total_hybrid_deployments

    from deployments
    group by 1, 2
)

select * from final
