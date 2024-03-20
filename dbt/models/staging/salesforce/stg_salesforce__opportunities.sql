select

    id as opportunity_id,
    account_id,
    contact_id,
    owner_id,

    name as opportunity_name,
    description,
    stage_name,
    amount,
    probability,
    close_date,
    type as opportunity_type,
    next_step,
    lead_source,

    is_closed,
    is_won,
    is_deleted,
    created_date as created_at,

    loss_reason_c as loss_reason,
    loss_details_c as loss_details,
    competitor_c as competitor,
    manual_forecast_category_c as manual_forecast_category,
    arr_c as arr,
    arr_new_c as new_arr,
    prior_term_arr_c as prior_term_arr,
    sal_date_c as sal_date,
    account_source_c as account_source,

    pre_opportunity_date_c as pre_opportunity_date,
    discovery_date_c as discovery_date,
    evaluation_date_c as evaluation_date,
    proposal_date_c as proposal_date,
    negotiation_review_date_c as negotiation_review_date

from {{ source('salesforce', 'opportunity') }}
where not is_deleted
