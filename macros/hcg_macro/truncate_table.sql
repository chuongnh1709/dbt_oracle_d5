-- for Oracle 

{% macro truncate_table(p_table_owner, p_table_name) %}

-- declare
{% set truncate_sql %}
    truncate table {{ p_table_owner }}.{{ p_table_name }} 
{% endset %}

{% set now = modules.datetime.datetime.now() %}

-- execute truncate 
{% do run_query(truncate_sql) %}

{% endmacro %}