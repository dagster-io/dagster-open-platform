-- Fails if any distinct maxio_contract_id in salesforce_maxio_transactions
-- has no corresponding row in salesforce_maxio_contract.
-- These contracts would be excluded from dim_contracts (which is based on the contract object).
select
    t.maxio_contract_id
from {{ ref('salesforce_maxio_transactions') }} t
left join {{ ref('salesforce_maxio_contract') }} c
    on t.maxio_contract_id = c.maxio_contract_id
where t.maxio_contract_id is not null
    and c.maxio_contract_id is null
group by t.maxio_contract_id
