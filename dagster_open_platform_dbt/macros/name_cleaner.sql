{#
    Purpose: Cleans a name field by applying the following rules:
      - If the value contains '<' or '>', returns 'unknown'.
      - If the value contains parentheses, removes the parentheses and all text inside (including any leading whitespace).
      - Removes all straight and curly double quotes: ", ", and ".
      - If none of the above, returns the original value with quotes removed.
#}
{% macro name_cleaner(name_col) %}
    case
        when {{ name_col }} like '%<%' or {{ name_col }} like '%>%' then 'unknown'
        else regexp_replace(
            case
                when {{ name_col }} like '%(%' then regexp_replace({{ name_col }}, '\\s*\\(.*\\)', '')
                else {{ name_col }}
            end,
            '[\"“”]', ''
        )
    end
{% endmacro %} 