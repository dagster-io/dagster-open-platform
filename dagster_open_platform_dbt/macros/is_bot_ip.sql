{% macro is_bot_ip(ip_column) -%}
  {% set prefixes = query_to_list("select ip_prefix from " ~ ref('bot_ip_prefixes')) %}
  {% if prefixes | length > 0 %}
  (
    {% for prefix in prefixes %}
      {{ ip_column }} like '{{ prefix }}'
      {% if not loop.last %} or {% endif %}
    {% endfor %}
  )
  {% else %}
  false
  {% endif %}
{%- endmacro %}
