with inv_amounts as (

    select

        invoice_created_at,
        status as invoice_status,
        invoice_id,
        customer_id,
        customer_name,
        sum(invoice_total_dollars) as invoice_total

    from {{ ref('stg_stripe__invoices') }}
    group by all
),

org_mapping as (

    select distinct

        ili.invoice_id,
        c.organization_id,
        o.organization_name

    from {{ ref('stg_stripe__invoice_line_items') }} as ili
    inner join {{ ref('stg_stripe__invoices') }} as i using (invoice_id)
    inner join {{ ref('stg_postgres__customer_info') }} as c on i.customer_id = c.stripe_customer_id
    inner join
        {{ ref('stg_postgres__organizations') }} as o
        on c.organization_id = o.organization_id
    where c.organization_id is not null

),

agent as (
    select
        organization_id,
        agent_type
    from {{ ref('fct_runs') }}
    group by 1, 2
    qualify row_number() over (partition by organization_id order by count(*) desc) = 1
),

org_plans as (

    select
        organization_id,
        plan_type
    from {{ ref('stg_postgres__organizations') }}
),


final as (
    select

        inv_amounts.invoice_created_at,
        inv_amounts.invoice_status,
        inv_amounts.customer_id,
        inv_amounts.customer_name,
        inv_amounts.invoice_id,
        org_mapping.organization_id,
        org_mapping.organization_name,
        coalesce(agent.agent_type, 'unknown agent') as agent_type,
        org_plans.plan_type,
        inv_amounts.invoice_total

    from inv_amounts
    left join org_mapping using (invoice_id)
    left join agent using (organization_id)
    left join org_plans using (organization_id)
    where inv_amounts.invoice_total > 0
)

select * from final
