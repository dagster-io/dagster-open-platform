{% macro parse_domain_from_email(email_column) -%}

    case 
        when {{ email_column }} is null then null 
        else split_part({{ email_column }}, '@', 2) 
    end

{%- endmacro %} 
