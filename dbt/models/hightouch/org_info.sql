with

{% set query %}
    select role_name
    from {{ ref('permission_levels') }}
{% endset %}

{% set role_list = query_to_list(query) %}

organizations as (
    select
        organization_id,
        organization_name,
        salesforce_account_id,
        stripe_customer_id,
        is_active as is_active_org
    from {{ ref('dim_organizations') }}
    where salesforce_account_id is not null
),

org_user_counts as (
    select
        organization_id,
        count(*) as total_user_count
    from {{ ref('user_organizations') }}
    group by 1
),

org_role_count_long as (
    select
        organization_id,
        role_name,
        count(*) as count
    from {{ ref('user_org_permissions') }}
    group by 1, 2
),

org_role_count as (
    select
        organization_id,
    {% for role in role_list %}
        zeroifnull({{ role }}_COUNT) as {{ role }}_COUNT{% if not loop.last %},{% endif %}
    {% endfor %}
    from org_role_count_long
    pivot
    (sum(count) for role_name in (
        {% for role in role_list %}
            '{{ role }}'{% if not loop.last %},{% endif %}
        {% endfor %}
    )) as p (
        organization_id,
        {% for role in role_list %}
            {{ role }}_COUNT{% if not loop.last %},{% endif %}
        {% endfor %}
    )
),

last_invoice as (
    select
        organization_id,
        date(invoice_created_at) as last_invoice_date,
        round(invoice_total, 2) as last_invoice_amount
    from {{ ref('stripe_invoices') }}
    qualify row_number() over (partition by organization_id order by invoice_created_at desc) = 1
),

trials_dates as (
    select
        customer_id as stripe_customer_id,
        trial_start,
        trial_end
    from {{ ref('stripe_subscriptions') }}
    where trial_start is not null
),

running_credits_sum as (
    select
        credits_date,
        organization_id,
        sum(total_credits_used)
            over (partition by organization_id order by credits_date)
            as running_credits_used
    from {{ ref('credit_utilization') }}
),

contract_credits_used as (
    select
        organization_id,
        running_credits_used
    from running_credits_sum
    where credits_date = current_date - 1
)

select
    organization_id,
    organization_name,
    salesforce_account_id,
    stripe_customer_id,
    is_active_org,
    running_credits_used as contract_credits_used,
    total_user_count,
    {% for role in role_list %}
        {{ role }}_COUNT,
    {% endfor %}
    last_invoice_amount,
    last_invoice_date,
    trial_start as trial_start_date,
    trial_end as trial_end_date
from
    organizations
inner join org_user_counts using (organization_id)
inner join org_role_count using (organization_id)
left join last_invoice using (organization_id)
left join contract_credits_used using (organization_id)
left join trials_dates using (stripe_customer_id)
qualify
    row_number() over (partition by organization_id order by trial_start_date desc nulls last) = 1
