{% test is_numeric(model, column_name) %}

with validation as (

    select
        {{ column_name }} as numeric_field

    from {{ model }}

),

validation_errors as (

    select
        numeric_field

    from validation

    where to_numeric(numeric_field) is null

)

select *
from validation_errors

{% endtest %}
