{% macro is_personal_email_domain(email_column) -%}

    case 
        when {{ parse_domain_from_email(email_column) }} in (
            select email_domain 
            from {{ ref('personal_email_domains') }}
        ) then true 
        else false 
    end

{%- endmacro %}
