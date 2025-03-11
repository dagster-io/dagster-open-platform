{% macro is_engaged(intent_score) -%}
    case 
        when {{intent_score}} >= 20 then true 
        else false
        end
{%- endmacro %}