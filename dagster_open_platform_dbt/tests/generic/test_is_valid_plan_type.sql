{% test is_valid_plan_type(model, column_name) %}

select
    split_part({{ column_name }}, '_', 1) as base_plan_type
from {{ model }}
where base_plan_type not in (
    'ENTERPRISE',
    'OPPORTUNITY',
    'PARTNER',
    'STANDARD',
    'TEAM',
    'SOLO'
)

{% endtest %}
