{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if target.name in ('prod', 'branch_deployment', 'dogfood', 'sdf', 'dev', 'purina_ci') and custom_schema_name is none -%}
        {% if node.fqn[2:-1]|length == 0 or node.fqn | length <= 2 %}
            {{ default_schema }}    
        {% else %}
            {% set prefix = node.fqn[2:-1]|join('_') %}
            {{ prefix | trim }}
        {% endif %}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}