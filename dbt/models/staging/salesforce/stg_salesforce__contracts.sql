select

    id as contract_id,
    account_id,
    opportunity_c as opportunity_id,

    arr_c as arr,
    cloud_credits_c as cloud_credits_contracted,
    contract_number,
    contract_term,
    created_date,
    start_date as contract_start_date,
    end_date as contract_end_date,
    launcher_seats_c as launcher_seats,
    pricing_model_c as pricing_model,
    serverless_c as is_serverless,
    status as contract_status


from {{ source('salesforce', 'contract') }}
where not is_deleted
