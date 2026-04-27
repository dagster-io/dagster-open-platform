{% macro dbt_backfill_enabled() %}
  {{ return(var('backfill', false)) }}
{% endmacro %}

{% macro configure_dbt_backfill_session() %}
  {% if dbt_backfill_enabled() %}
    {% set warehouse = var('backfill_snowflake_warehouse', 'BACKFILL_WH') %}
    {% set statement_timeout_seconds = var('backfill_statement_timeout_seconds', 86400) %}
    {% do run_query('use warehouse ' ~ warehouse) %}
    {% do run_query(
      'alter session set STATEMENT_TIMEOUT_IN_SECONDS = ' ~ statement_timeout_seconds
    ) %}
  {% endif %}
{% endmacro %}
