-- us_only: refs models (fct_organization_contracts) that are not built in the EU
-- deployment; the EU dbt invocations exclude tag:us_only so eager indirect
-- selection doesn't pull this test in via the eu-tagged dim_contracts.
{{ config(tags=["us_only"]) }}

-- Fails if an org in fct_organization_contracts is attributed to a contract whose
-- dim_contracts.maxio_contract_organization_id is explicitly set to a *different*
-- org. This is the exact shape of the original multi-org bug: legora-analytics was
-- getting attributed to contract 22172 even though that contract is explicitly
-- mapped to leya.
--
-- Note this is NOT a "no overlapping contracts per org" test -- an org legitimately
-- having two contracts with overlapping date ranges during a renewal transition is
-- expected and handled elsewhere (is_current_contract / is_most_recent_prior_contract
-- in dim_contracted_usage_by_day), not a bug.

select
    oc.organization_id,
    oc.contract_id,
    c.maxio_contract_organization_id
from {{ ref('fct_organization_contracts') }} oc
inner join {{ ref('dim_contracts') }} c
    on c.contract_id = oc.contract_id
where c.maxio_contract_organization_id is not null
    and c.maxio_contract_organization_id::varchar != oc.organization_id::varchar
