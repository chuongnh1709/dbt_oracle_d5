/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 21-Nov-2023
  - updated at        : 01-Jan-3000
  - this macro used for Oracle 
*/

{% macro alter_session_parallel(p_query_degree=4, p_dml_degree=p_query_degree, p_ddl_degree=p_query_degree) %}
  {%- if execute -%}

    {% set enable_query_parallel %}
        alter session force parallel query parallel {{ p_query_degree }}
    {% endset %}

    {% set enable_dml_parallel %}
        alter session force parallel dml parallel {{ p_dml_degree }}
    {% endset %}

    {% set enable_ddl_parallel %}
        alter session force parallel ddl parallel {{ p_ddl_degree }}
    {% endset %}

    {% set now = modules.datetime.datetime.now() %}

    {% do run_query(enable_query_parallel) %}
    {% do run_query(enable_dml_parallel) %}
    {% do run_query(enable_ddl_parallel) %}

  {%- endif -%}
{% endmacro %}