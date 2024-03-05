{{ config(materialized='table') }}

select
    id as account_id,
    organization_id__c as organization_id,
    ownerid as owner_id,
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
    annualrevenue as annual_revenue,
    numberofemployees as number_of_employees,
    description,
    accountsource,
    source__c as account_source_custom,
    status__c as account_status,
    cloud_credits__c as cloud_credits,
    createddate as created_at,
    isdeleted as is_deleted

from {{ source('salesforce', 'account') }}
where not is_deleted and (organization_id regexp '\\d+' or organization_id is null)
