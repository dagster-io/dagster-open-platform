{% macro salesforce_opportunity_new_arr(
    opportunity_type_column,
    li_prorated_product_count_column,
    li_non_prorated_product_count_column,
    li_total_annualized_arr_column,
    amount_year1_column,
    prior_term_arr_column,
    term_months_column
) %}
    case
        --- mixed proration upsell: has both prorated and non-prorated products; use total annualized arr minus prior term arr
        when {{ opportunity_type_column }} = 'Upsell'
            and {{ li_prorated_product_count_column }} > 0
            and {{ li_non_prorated_product_count_column }} > 0
            then {{ li_total_annualized_arr_column }} - {{ prior_term_arr_column }}
        --- non-prorated upsell: all products are non-prorated; use year 1 minus prior term arr without annualizing
        when {{ opportunity_type_column }} = 'Upsell'
            and {{ li_prorated_product_count_column }} = 0
            and {{ li_non_prorated_product_count_column }} > 0
            then {{ amount_year1_column }} - {{ prior_term_arr_column }}
        --- always take the full year 1 amount for new business
        when {{ opportunity_type_column }} = 'New Business' then {{ amount_year1_column }}
        --- if the term is a multiple of 12, then we can just subtract the prior term arr without needing to annualize
        when mod({{ term_months_column }}, 12) = 0 then {{ amount_year1_column }} - {{ prior_term_arr_column }}
        --- if the term is not a multiple of 12, then we need to annualize the amount and subtract the prior term arr
        else ({{ amount_year1_column }} * (12 / mod({{ term_months_column }}, 12))) - {{ prior_term_arr_column }}
    end
{% endmacro %}
