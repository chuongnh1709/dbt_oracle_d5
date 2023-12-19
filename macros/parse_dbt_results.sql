{% macro parse_dbt_results(results) %}
    
    -- Create a list of parsed results
    {%- set parsed_results = [] %}

    -- Flatten results and add to list
    {% for run_result in results %}

        -- Convert the run result object to a simple dictionary
        {% set run_result_dict = run_result.to_dict() %}

        -- Get the underlying dbt graph node that was executed
        {% set node = run_result_dict.get('node') %}
        {% set rows_affected = run_result_dict.get('adapter_response', {}).get('rows_affected', 0) %}
        {%- if not rows_affected -%}
            {% set rows_affected = 0 %}
        {%- endif -%}

        {#-- Get execute timming : started_at ; completed_at
        #}

        {% set timing = run_result_dict.get('timing', {}) %} -- list object

        {% for item in timing %}
          {% set item_dict = item %}
          {% set timing_name =  item_dict.get('name') %}

          {% if timing_name == "execute" %}
            {% set started_at = item_dict.get('started_at') %}
            {% set completed_at = item_dict.get('completed_at') %}

            {% set parsed_result_dict = {
              'result_id': invocation_id ~ '.' ~ node.get('unique_id')
              ,'invocation_id': invocation_id
              ,'unique_id': node.get('unique_id')
              ,'database_name': node.get('database')
              ,'schema_name': node.get('schema')
              ,'name': node.get('name')
              ,'resource_type': node.get('resource_type')
              ,'status': run_result_dict.get('status')
              ,'execution_time': run_result_dict.get('execution_time')
              ,'rows_affected': rows_affected
              ,'message': run_result_dict.get('message')
              ,'started_at': started_at
              ,'completed_at': completed_at
            }%}
            {% do parsed_results.append(parsed_result_dict) %}

          {% endif %}

      {% endfor %}
    {% endfor %}

    {{ return(parsed_results) }}

{% endmacro %} 

