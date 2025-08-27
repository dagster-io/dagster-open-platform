{% macro clean_trailing_path_values(path_column) %}
    CASE 
        WHEN {{ path_column }} = '/' THEN '/'
        ELSE rtrim({{ path_column }}, '/')
    END
{% endmacro %}
