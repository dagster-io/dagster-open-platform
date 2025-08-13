--- Please note the following specific email addresses that are internal users:
--- pedram@pedramnavid.com = Pedram's personal email
--- justin@novotta.com = Contract developer that works on our website

{% macro is_internal_user(email) -%}

    case 
        when {{ parse_domain_from_email(email) }} in (
            select email_domain 
            from {{ ref('internal_email_domains') }}
        ) then true
        when {{email}} = 'pedram@pedramnavid.com' then true
        when {{email}} = 'justin@novotta.com' then true
        else false
    end

{%- endmacro %}