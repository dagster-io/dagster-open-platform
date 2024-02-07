select

    id as contract_id,
    accountid as account_id,
    opportunity__c as opportunity_id,

    arr__c as arr,
    cloud_credits__c as cloud_credits_contracted,
    contractnumber as contract_number,
    contractterm as contract_term,
    createddate as created_date,
    startdate as contract_start_date,
    enddate as contract_end_date,
    launcher_seats__c as launcher_seats,
    pricing_model__c as pricing_model,
    serverless__c as is_serverless,
    status as contract_status


from {{ source('salesforce', 'contract') }}
where not isdeleted
