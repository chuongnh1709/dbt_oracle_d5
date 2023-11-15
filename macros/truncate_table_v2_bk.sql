-- for Oracle 

{% macro truncate_table_v2_bk(p_table_owner, p_table_name) %}

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
  where table_name = upper( '{{ p_table_name }}' )
    and owner = upper( '{{ p_table_owner }}' )
{% endset %}

{% set chk_tbl2 %}
  select 
    case when count(1) = 0 then null 
    else 1 
    end as cnt
  from dba_tables 
  where table_name = upper( '{{ p_table_name }}' )
    and owner = upper( '{{ p_table_owner }}' )
{% endset %}


{% set results  = run_query(chk_tbl2) %}

{% set now = modules.datetime.datetime.now() %}

-- begin
{% if execute %}
 {% do log("[INFO1] Starting truncate table " ) %}
  {% set results_list = results.columns[0].values() %}
    {% if results|length > 0 %}
        {% do log("[INFO1] Sql statement : " ~ truncate_sql , info=True) %}
        {# do run_query(truncate_sql) #}
        {% do log("[INFO1] Table " ~ p_table_name ~ " truncated at " ~ now , info=True) %}
    {% else %}
      {% do log("[INFO2] Table does not exists ! " , info=True) %}
    {% endif %}
{% endif %}

-- end
{% endmacro %}



