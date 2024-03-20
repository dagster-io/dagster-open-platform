with checklist as (
    select * from {{ ref('stg_postgres__onboarding_checklist') }}
),

orgs as (
    select * from {{ ref('dim_organizations') }}
),

final as (

    select

        checklist.organization_id,
        checklist.entry_key,
        checklist.status,
        date_trunc('day', checklist.created_at) as event_created_day,
        count(checklist.primary_id) as total_events,

        /* TODO: Confirm if we want created or updated here */
        sum(datediff('second', orgs.org_created_at, checklist.updated_at)) as sum_time_to_event,
        avg(datediff('second', orgs.org_created_at, checklist.updated_at)) as avg_time_to_event

    from checklist
    inner join orgs using (organization_id)
    group by all
)

select * from final
