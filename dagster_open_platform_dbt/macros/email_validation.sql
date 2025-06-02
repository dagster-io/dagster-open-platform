{% macro email_structure_validation(email) -%}

    case 
        when {{ email }} RLIKE '^[a-zA-Z0-9._%+''-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'
        then true
        else false
    end

{%- endmacro %}