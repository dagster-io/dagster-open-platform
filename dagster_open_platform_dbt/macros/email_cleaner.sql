{% macro email_cleaner(email) -%}

        nullif(trim(lower({{email}})), 'null')

{%- endmacro %}