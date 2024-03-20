{{ config(materialized='table') }}

select
    id as account_id,
    organization_id_c as organization_id,
    owner_id,
    name as account_name,
    type as account_type,
    website as website_unparsed,
    --    Retrieves the domain.com portion of a website
    --       │Optionally capture http(s)://  and discard
    --       │
    --       │             Optionally capture www. and discard
    --       │             │
    --       │             │      Capture the domain.tld portion
    --       │             │      │Matching as many letters before a dot
    --       │             │      │And 2 or more letters after the dot (the tld)
    --       ▼             ▼      ▼
    --    (http[s]:\/\/)?(w*\.)?(\w*\.\w{2,})
    regexp_substr(website, $$(http[s]:\/\/)?(w*\.)?(\w*\.\w{2,})$$, 1, 1, 'e', 3) as website,
    industry,
    annual_revenue,
    number_of_employees,
    description,
    account_source,
    source_c as account_source_custom,
    status_c as account_status,
    cloud_credits_c as cloud_credits,
    created_date as created_at,
    is_deleted

from {{ source('salesforce', 'account') }}
where not is_deleted and (organization_id regexp '\\d+' or organization_id is null)
