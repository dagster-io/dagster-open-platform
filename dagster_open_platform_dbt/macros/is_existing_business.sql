{% macro is_existing_business(opportunity_type_column) %}
    case 
        when {{ opportunity_type_column }} = 'Upsell' then true
        when {{ opportunity_type_column }} = 'Renewal' then true 
        when {{ opportunity_type_column }} = 'Existing Business' then true 
        else false 
    end
{% endmacro %} 