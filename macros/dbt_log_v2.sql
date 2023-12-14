{#  
  Updated by : Chuong.Nguyen - DM2 
  Updated at : 14/12/2023
  Used for : insert brief info of model run-time into dbt_log table 
  DDL script in 03_#incremental/_dbt_log/dbt_log.sql 
  -- Trying to generate Model_id which is the same for log_start and log_end -- Working / developing 
#}

{% macro dbt_log_v2(p_status ,p_log_table='dbt_log') %}

  {%- if execute -%}
    
    {#
    {%- set current_milli_time = round(modules.datetime.datetime.now().timestamp() * 1000 , 0)  -%} 
    current_milli_time = {{ current_milli_time | round(0) }} 
    {% do log("[INFO] current_milli_time : " ~ this.table ~"_"~ current_milli_time , info=True) %}
    #}

    {%- set materialization = config.get('materialized') -%}
    {#  -- not work because can not compare with NULL 
    {%- if materialization | length  == 0 -%}
      {% set materialization = 'table' %}
    {%- endif -%}
    #}

    {% set insert_dbt_log_query -%}
        insert into {{ this.schema }}.{{ p_log_table }} (  model_schema ,model_name ,model_type ,model_status ) 
        values( '{{ this.schema }}' ,'{{ this.table }}' ,  decode('{{ materialization }}',null,'table') ,'{{ p_status }}' )
    {%- endset -%}

    {%- do run_query(insert_dbt_log_query) -%}
  
  {%- endif -%}

  -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
  {{ return ('') }}
    
{% endmacro %}
