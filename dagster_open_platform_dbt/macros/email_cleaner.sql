{% macro email_cleaner(email) -%}

        trim(lower({{email}}))

{%- endmacro %}