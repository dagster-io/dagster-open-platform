{% test is_valid_plan_type(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} not in (
    'ENTERPRISE',
    'OPPORTUNITY',
    'PARTNER',
    'STANDARD',
    'TEAM',
    'SOLO'
)

{% endtest %}
