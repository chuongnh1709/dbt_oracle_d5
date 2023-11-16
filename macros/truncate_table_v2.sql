{# [comment] for Oracle #}
{# [comment] handle table does not exists #}

{% macro truncate_table_v2(p_table_owner, p_table_name) %}

{% set truncate_sql %}
    truncate table {{ p_table_owner }}.{{ p_table_name }} 
{% endset %}

{% set chk_tbl %}
  select  count(1)  as cnt
  from dba_tables 
  where table_name = upper( '{{ p_table_name }}' )
    and owner = upper( '{{ p_table_owner }}' )
{% endset %}

{% do log('[INFO chk_tbl]  ' ~ chk_tbl , info=True) %}

{% if execute %}
  {% set results  = run_query(chk_tbl) %}
   
  {# [comment] get column 1 - row 1 #}
  {% set item = results.columns[0].values()[0] %} 

  {% set now = modules.datetime.datetime.now() %}

  {% if item > 0 %}  
      {% do log("[INFO] cnt : " ~ item , info=True) %}
      {% do run_query(truncate_sql) %}
      {% do log("[INFO] Table " ~ p_table_owner~'.'~p_table_name ~ " truncated at " ~ now , info=True) %}
  {% else %}
      {% do log("[INFO] Table "  ~ p_table_owner~'.'~p_table_name ~ " does not exists ! " , info=True) %}
  {% endif %}
{% endif %}

{% endmacro %}



