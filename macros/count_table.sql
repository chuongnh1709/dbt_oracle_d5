{% macro count_table() %}

{% set count_query %}
  SELECT count(*) as count from {{ this }}
{% endset %}

{% set results = run_query(count_query) %}

{% if execute %}
  {# [comment] Return the first column #}
  {% set results_list = results.columns[0].values() %}
  {{ log('Counting row for '~ this , True) }}
  {{ log(modules.datetime.datetime.now().strftime('%H:%M:%S') ~ ' | Number of rows : '~ results_list , True) }}
{% else %}
  {% set results_list = [] %}
{% endif %}

{% endmacro %}