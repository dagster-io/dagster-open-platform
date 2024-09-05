{% macro uri_percent_decode() %}

{%- call statement('get_current_role', fetch_result=True) -%}
      SELECT CURRENT_ROLE() 
{%- endcall -%}

{%- set role = load_result('get_current_role')['data'][0][0] -%}


{% set sql %}
create or replace function {{ target.database }}.utils.uri_percent_decode(VAL string)
returns string
language javascript
as $$
    try {
        decoded_uri = decodeURIComponent(VAL);
    } catch {
        decoded_uri = null;
    } finally {
        return decoded_uri;
    }
$$;
grant usage on function {{ target.database }}.utils.uri_percent_decode(string) to role {{ role }};
{% endset %}

{% do run_query(sql) %}

{% do log("Successfully created `o` and granted usage to TRANSFORMER_ROLE & DEV_TRANSFORMER_ROLE", info=True) %}

{% endmacro %}