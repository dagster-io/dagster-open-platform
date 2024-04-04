/* This is a SQL query that will return the organization_id of any organization
   that has had five successful runs in a row. It uses the match_recognize
   function to match patterns in the data.

   Incredibly, this runs in 15 seconds on 30 million rows, which justifies
   the complexity of the query vs self-joins.
   https://docs.snowflake.com/en/sql-reference/functions/match_recognize.html

   An active organization is defined as any organization that has had at least
   five successful runs in a row at least once. */

/* First, we create a table of all successful runs for each organization,
   and truncate dates. Distinct reduces the number of rows to one per
   organization per day. */

with contracts as ( 
    select * from {{ ref('stg_salesforce__contracts') }}
),

accounts as (
    select * from {{ ref('stg_salesforce__accounts') }}
),

opportunities as (
    select * from {{ ref('stg_salesforce__opportunities') }}
),

runs as (
    select * from {{ ref('fct_runs') }}
),

organizations as (
    select * from {{ ref('dim_organizations') }}
),

successive_runs as (
    select distinct
        organization_id,
        ended_at::date as end_dt,
        lead(end_dt) over (partition by organization_id order by end_dt) as lead_dt
    from runs
    where run_status = 'SUCCESS'
    -- We only want runs that end on a different day than the previous run
    qualify end_dt != lead_dt 
),

run_dates as (
    select distinct
        organization_id,
        start_date as successive_run_start_date,
        end_date as successive_run_end_date
    from successive_runs
    match_recognize (
        partition by organization_id
        order by end_dt
        measures
        match_number() as match_number,
        first(end_dt) as start_date,
        last(end_dt) as end_date,
        count(*) as rows_in_sequence
        all rows per match
        pattern (success { 4 })
        define success as datediff('day', (end_dt), lead(end_dt)) = 1
    )
),


/* We only want to look at engagement metrics for the first contract for a customer. 
We assume renewed customers are already engaged. We find the first contract using the subscription_index column.
We also remove contracts that have not yet started.
*/
new_subscriptions as (
    select 

        organizations.organization_id,
        organizations.organization_name,
        account_name,
        account_status,
        contracts.arr,
        cloud_credits_contracted,
        contract_term,
        contract_start_date,
        contract_end_date,
        datediff('days', contract_start_date, contract_end_date) as contract_length,
        launcher_seats,
        pricing_model,
        contract_status,
        row_number() over(
            partition by accounts.account_id order by contract_start_date
        ) as subscription_index

    from contracts
    join accounts using (account_id)
    join opportunities using (opportunity_id)
    join organizations on 
        accounts.organization_id = organizations.organization_id
        and contract_start_date < current_date()
    where opportunity_type = 'New Business'
    qualify subscription_index = 1
    order by organizations.organization_id, subscription_index
),

/* An engaged customer is defined as a customer who had five successive runs within 90 days of the contract start date */
engaged as (

    select distinct

        new_subscriptions.organization_id,
        organization_name,
        arr,
        cloud_credits_contracted,
        contract_start_date,
        contract_end_date,
        contract_term,
        pricing_model,
        contract_status,
        /* This is the date 90 days after the contract start date. Five runs must have been completed by this date. */
        dateadd('days', 90, contract_start_date) as engagement_marker_date,
        min(successive_run_end_date) over(partition by new_subscriptions.organization_id) as first_successive_run_end_date

    from new_subscriptions
    left join run_dates on
        run_dates.successive_run_start_date between contract_start_date and contract_end_date
        and new_subscriptions.organization_id = run_dates.organization_id
    order by 1 
),

final as (

    select

    organization_id,
    organization_name,
    arr,
    cloud_credits_contracted,
    contract_start_date,
    contract_end_date,
    contract_term,
    pricing_model,
    first_successive_run_end_date,
    engagement_marker_date,
    iff(first_successive_run_end_date <= engagement_marker_date, true, false) as is_90day_activated

    from engaged
)


select * from final 
