-- for Oracle 

{% macro truncate_table(p_table_owner, p_table_name) %}

{% set sql %}
    truncate table {{ p_table_owner }}.{{ p_table_name }} 
{% endset %}

{% set now = modules.datetime.datetime.now() %}

{% do log("[INFO] Sql statement : " ~ sql ) %}
{{dbt_utils.log_info('[LOG-INFO] Sql statement :'~ sql )}}

{% do run_query(sql) %}

{% do log("[INFO] Table " ~ p_table_name ~ " truncated at " ~ now , info=True) %}

{% endmacro %}