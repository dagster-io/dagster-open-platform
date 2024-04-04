{% macro limit_dates_for_insights(ref_date) -%}
{{ref_date}} >= dateadd('day', -{{var("insights_num_days_to_include")}}, current_date)
{%- endmacro %}
