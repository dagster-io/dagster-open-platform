CREATE OR REPLACE {{ type }} {% if database_name %}{{ database_name }}{% if schema_name %}.{{ schema_name }}{% endif %}.{% elif schema_name %} {{ schema_name }}.{% endif %}{{ name }}
    {%- if columns and columns|length > 0 %}
(
    {%- for column in columns %}
    {{ column.name }} {{ column.type }}{% if column.as %} AS {{ column.as }}{% endif %}{% if not loop.last %},{% endif %}
    {%- endfor %}
)
    {%- endif %}
{%- if partition_by %}
PARTITION BY ({{ partition_by }})
{%- endif %}
{%- if options %}
{% for option_name, option_value in options.items() %}
    {%- if option_value is mapping %}
    {{ option_name }} = (
        {%- for sub_name, sub_value in option_value.items() %}
        {{ sub_name }} = {{ sub_value|tojson }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    )
    {%- elif option_name == "location" and type|upper != "STAGE" %}
    {{ option_name }} = {{ option_value }}
    {%- elif option_value is not none %}
    {{ option_name }} = {{ option_value|tojson }}
    {%- endif %}
{% endfor %}
{%- endif %}
