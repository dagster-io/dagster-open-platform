{% macro coalesce_referrer_from_search(search_field, referrer_field) -%}
    {#-- Maps search ref parameters to their corresponding referrer URLs
         when the referrer field is null.
         
         Example usage: coalesce_referrer_from_search('search', 'referrer')
    --#}
    case 
        when {{ search_field }} = '?ref=producthunt' then coalesce({{ referrer_field }}, 'https://www.producthunt.com/')
        when {{ search_field }} = '?ref=blef.fr' then coalesce({{ referrer_field }}, 'https://www.blef.fr/')
        when {{ search_field }} = '?ref=blog.langchain.dev' then coalesce({{ referrer_field }}, 'https://blog.langchain.dev/')
        when {{ search_field }} = '?ref=stackshare' then coalesce({{ referrer_field }}, 'https://stackshare.io/')
        when {{ search_field }} = '?ref=ssp.sh' then coalesce({{ referrer_field }}, 'https://www.ssp.sh/')
        when {{ search_field }} = '?ref=dedp.online/' then coalesce({{ referrer_field }}, 'https://www.dedp.online/')
        when {{ search_field }} = '?ref=todobi.com' then coalesce({{ referrer_field }}, 'https://todobi.com/dagster/')
        else {{ referrer_field }}
    end
{%- endmacro %}

