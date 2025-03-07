{% macro generate_database_name(custom_database_name=none, node=none) -%}
    {%- set default_database = target.database -%}
    {%- if target.name in ('prod', 'branch_deployment', 'dogfood', 'sdf') and custom_database_name is none -%}
        {% if node.fqn|length <= 2 %}
            {{ default_database }}
        {% elif target.name == 'branch_deployment' %}
            {% set prefix = node.fqn[1] %}
            {{ return([prefix | trim, 'clone', env_var('DAGSTER_CLOUD_PULL_REQUEST_ID')]|join('_')) }}
        {% else %}
            {% set prefix = node.fqn[1] %}
            {{ prefix | trim }}
        {% endif %}
    {%- else -%}
        {{ default_database }}
    {%- endif -%}

{%- endmacro %}