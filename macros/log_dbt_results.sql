{#  
  Updated by : Chuong.Nguyen - DM2 
  Updated at : 13/12/2023
  Description : This Macro was customized for Oracle DB  
#}

{% macro log_dbt_results(results) %}
    -- depends_on: {{ ref('dbt_model_results') }}
    {%- if execute -%}
        {%- set parsed_results = parse_dbt_results(results) -%}
        {%- if parsed_results | length  > 0 -%}
            {% set insert_dbt_results_query -%}
                insert ALL 
                {% for parsed_result_dict in parsed_results %}
                  into {{ ref('dbt_model_results') }}
                      (
                          started_at
                          ,completed_at
                          ,result_id
                          ,invocation_id
                          ,unique_id
                          ,database_name
                          ,schema_name
                          ,name
                          ,resource_type
                          ,status
                          ,execution_time
                          ,rows_affected
                          ,message
                  ) values
                      (
                           case
                            when to_char('{{ parsed_result_dict.get('started_at') }}') = 'None' then date'1000-01-01'
                            else to_timestamp('{{ parsed_result_dict.get('started_at') }}' ,'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH')
                           end 
                          ,case
                            when to_char('{{ parsed_result_dict.get('completed_at') }}') = 'None' then date'1000-01-01'
                            else to_timestamp('{{ parsed_result_dict.get('completed_at') }}' ,'YYYY-MM-DD"T"HH24:MI:SS.FF6TZH')
                           end 
                          ,'{{ parsed_result_dict.get('result_id') }}'
                          ,'{{ parsed_result_dict.get('invocation_id') }}'
                          ,'{{ parsed_result_dict.get('unique_id') }}'
                          ,'{{ parsed_result_dict.get('database_name') }}'
                          ,'{{ parsed_result_dict.get('schema_name') }}'
                          ,'{{ parsed_result_dict.get('name') }}'
                          ,'{{ parsed_result_dict.get('resource_type') }}'
                          ,'{{ parsed_result_dict.get('status') }}'
                          ,{{ parsed_result_dict.get('execution_time') }}
                          ,{{ parsed_result_dict.get('rows_affected') }}
                          /* ,'{{ parsed_result_dict.get('message') }}' */
                          , REGEXP_SUBSTR ( '{{ parsed_result_dict.get('message') }}' , '.*$', 1, 1, 'm') 
                            || CHR (10) 
                            || REGEXP_SUBSTR ( '{{ parsed_result_dict.get('message') }}' , '.*$', 1, 3, 'm')
                            -- get 2 first lines only 
                      )  
                {% endfor %}
                SELECT * FROM dual

          {%- endset -%}
          {%- do run_query(insert_dbt_results_query) -%}
        {%- endif -%}
    {%- endif -%}

    -- This macro is called from an on-run-end hook and therefore must return a query txt to run. Returning an empty string will do the trick
    {{ return ('') }}
    
{% endmacro %}