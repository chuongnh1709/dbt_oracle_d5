{% macro truncate_table(p_table_owner, p_table_name) %}

{% set sql %}
    truncate table {{ p_table_owner }}.{{ p_table_name }} 
{% endset %}

{% set now = modules.datetime.datetime.now() %}

{% do log("[INFO] Sql statement : " ~ sql , info=True) %}

{% do run_query(sql) %}

{% do log("[INFO] Table " ~ p_table_name ~ " truncated at " ~ now , info=True) %}

{% endmacro %}