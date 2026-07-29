-- us_only: refs dim_accounts, which is not built in the EU deployment; the EU
-- dbt invocations exclude tag:us_only so eager indirect selection doesn't pull
-- this test in via its eu-tagged parents (dim_contracts,
-- fct_organization_contracts, fct_contract_organization_credits).
{{ config(tags=["us_only"]) }}

-- Fails if a contract on an account with a primary org (dim_accounts.organization_id),
-- and with no transaction- or contract-level org signal of its own, has no row in
-- fct_organization_contracts -- i.e. tier 3 (the primary-org fallback) should have
-- resolved it but didn't.
--
-- This exists because that failure mode has a real precedent: a silent type
-- mismatch in the tier resolution (a NUMBER/VARCHAR UNION coercion) made every
-- tier 2/3 equality check fail without raising any error, dropping matches from
-- ~477 to 0. This test catches exactly that class of bug by checking coverage,
-- not specific values, so it doesn't need updating as contracts renew or expire.

with dim_accounts as (
    select * from {{ ref('dim_accounts') }}
    where organization_id is not null
),

contract_organization_allocations as (
    select * from {{ ref('fct_contract_organization_credits') }}
),

contracts_needing_fallback as (
    select
        c.contract_id,
        c.account_id
    from {{ ref('dim_contracts') }} c
    inner join dim_accounts a
        on a.account_id = c.account_id
    left join contract_organization_allocations coa
        on coa.contract_id = c.contract_id
    where c.contract_id is not null
        and c.maxio_contract_organization_id is null
        and coa.contract_id is null
    group by 1, 2
)

select
    cnf.contract_id,
    cnf.account_id
from contracts_needing_fallback cnf
left join {{ ref('fct_organization_contracts') }} oc
    on oc.contract_id = cnf.contract_id
where oc.contract_id is null
