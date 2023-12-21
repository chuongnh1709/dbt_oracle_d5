-- Check is_Sarturday

-- if weekno < 5:
--     print "Weekday"
-- else:  # 5 Sat, 6 Sun
--     print "Weekend"

-- Use the date.weekday() method. Digits 0-6 represent the consecutive days of the week, starting from Monday.

{% macro is_today_xday_of_week() %}
  {% set now = modules.datetime.datetime.today().weekday() %}
  {% if now == 6 %}
    {% do return(True) %}
  {% else %}
    {% do return(False) %}
  {% endif %}
{% endmacro %}