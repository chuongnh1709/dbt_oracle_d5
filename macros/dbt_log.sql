{#  
  Updated by : Chuong.Nguyen - DM2 
  Updated at : 14/12/2023
  Used for : insert brief info of model run-time into dbt_log table 
  DDL script in 03_#incremental/_dbt_log/dbt_log.sql 
#}

{% macro dbt_log(p_status, p_log_table='dbt_log') %}

  {%- if execute -%}
    {%- set materialization = config.get('materialized') -%}

    {#
    {% do log("[INFO] materialization : " ~  materialization , info=True) %}
    #}
    
    {% set insert_dbt_log_query -%}
        insert into {{ this.schema }}.{{ p_log_table }} 
          (
            model_schema 
            ,model_name 
            ,model_type 
            ,model_status 
          ) 
        values( 
          '{{ this.schema }}' 
          ,'{{ this.table }}' 
          ,nvl('{{ materialization }}','table') 
          ,'{{ p_status }}' 
          )
    {%- endset -%}

    {%- do run_query(insert_dbt_log_query) -%}
  
  {%- endif -%}

  -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
  {{ return ('') }}
    
{% endmacro %}
