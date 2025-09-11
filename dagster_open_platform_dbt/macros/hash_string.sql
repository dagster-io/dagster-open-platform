{% macro hash_string(input_string) -%}
    SHA2({{ input_string }}, 512)
{%- endmacro %}