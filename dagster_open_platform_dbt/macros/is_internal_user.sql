{% macro is_internal_user(email) -%}

    case 
        when {{email}} like '%@elementl.%' then true
        when {{email}} like '%@dagsterlabs.%' then true
        else false
    end

{%- endmacro %}