-- for Oracle 

{% macro truncate_table_v2(p_table_owner, p_table_name) %}

-- declare
{% set truncate_sql %}
    truncate table {{ p_table_owner }}.{{ p_table_name }} 
{% endset %}

{% set chk_tbl %}
  select 
    case when count(1) = 0 then null 
    else 1 
    end as cnt
  from dba_tables 
  where table_name = upper( {{ p_table_name }})
    and owner = upper( {{ p_table_owner }} )
{% endset %}

{% set results  = run_query(chk_tbl) %}

{% set now = modules.datetime.datetime.now() %}

-- begin
{% if execute %}
 {% do log("[INFO] Starting truncate table " ) %}
  {% set results_list = results.columns[0].values() %}
    {% if results|length > 0 %}
        {% do log("[INFO] Sql statement : " ~ truncate_sql ) %}
        {% do run_query(truncate_sql) %}
        {% do log("[INFO] Table " ~ p_table_name ~ " truncated at " ~ now , info=True) %}
    {% else %}
      {% do log("[INFO] Table does not exists ! " ) %}
    {% endif %}
{% endif %}

-- end
{% endmacro %}