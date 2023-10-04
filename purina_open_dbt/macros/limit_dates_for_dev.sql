{% macro limit_dates_for_dev(ref_date) -%}
{% if
    target.name in ['branch_deployment', 'personal']
    and var("ignore_date_limits", false) is not true
-%}
    {{ref_date}} >= dateadd('day', -{{var("dev_num_days_to_include")}}, current_date)
{% else -%}
    TRUE
{%- endif %}
{%- endmacro %}
