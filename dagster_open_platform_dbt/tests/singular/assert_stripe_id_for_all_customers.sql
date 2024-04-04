select
    *  
from {{ source("cloud_product", "customer_info" )}} customer_info 
    inner join {{ ref("dim_organizations") }} dim_organizations using (organization_id) 
where
    dim_organizations.plan_type <> 'PARTNER'
    and dim_organizations.status <> 'READ_ONLY'
    and not is_internal
    and stripe_customer_id is null