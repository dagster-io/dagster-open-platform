{% macro generate_business_unit_flags(assigned_business_unit_ids_field) %}
    -- Generate business unit flags by using a pivot approach with the business units seed
    -- This creates flags for each business unit defined in the seed file
    {% set business_units_query %}
        select business_unit_id, business_unit_name from {{ ref('business_units') }}
    {% endset %}
    
    {% set business_units = run_query(business_units_query) %}
    
    {% if business_units %}
        {% for business_unit in business_units %}
            max(case when array_contains('{{ business_unit[0] }}'::variant, split({{ assigned_business_unit_ids_field }}, ';')) then true else false end) as is_{{ business_unit[1].lower().replace(' ', '_') }}_business_unit{% if not loop.last %},{% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro get_business_unit_names(assigned_business_unit_ids_field) %}
    -- Get business unit names by joining with the business units seed
    listagg(distinct business_units.business_unit_name, '; ') within group (order by business_units.business_unit_name) as business_unit_names
{% endmacro %}
