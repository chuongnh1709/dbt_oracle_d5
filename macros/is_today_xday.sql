{% macro is_today_xday() %}
  {% set now = modules.datetime.datetime.now().day %}
  {% if now == 14 %}
      {% do return(True) %}
  {% else %}
    {% do return(False) %}
  {% endif %}
{% endmacro %}
