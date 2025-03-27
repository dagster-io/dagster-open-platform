{% macro is_intent_score_greater_than_zero(intent_score) -%}
    case 
        when {{intent_score}} > 0 then true
        else false
        end 
{%- endmacro %}