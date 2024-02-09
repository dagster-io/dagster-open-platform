select

    id as opportunity_id,
    accountid as account_id,
    contactid as contact_id,
    ownerid as owner_id,

    name as opportunity_name,
    description,
    stagename as stage_name,
    amount,
    probability,
    closedate as close_date,
    type as opportunity_type,
    nextstep as next_step,
    leadsource as lead_source,

    isclosed as is_closed,
    iswon as is_won,
    isdeleted as is_deleted,
    createddate as created_at,

    loss_reason__c as loss_reason,
    loss_details__c as loss_details,
    competitor__c as competitor,
    manual_forecast_category__c as manual_forecast_category,
    arr__c as arr,
    new_arr__c as new_arr,
    prior_term_arr__c as prior_term_arr,
    sal_date__c as sal_date,
    account_source__c as account_source,

    pre_opportunity_date__c as pre_opportunity_date,
    discovery_date__c as discovery_date,
    evaluation_date__c as evaluation_date,
    proposal_date__c as proposal_date,
    negotiation_review_date__c as negotiation_review_date

from {{ source('salesforce', 'opportunity') }}
where not is_deleted
