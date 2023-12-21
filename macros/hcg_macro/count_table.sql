{% macro count_table() %}

{% set count_query %}
  SELECT count(*) as count from {{ this }}
{% endset %}

{% set results = run_query(count_query) %}

{% if execute %}
  {# [comment] Return the first column , first row #}
  {% set item = results.columns[0].values()[0] %}
  {{ log('Counting row for '~ this , True) }}
  {{ log(modules.datetime.datetime.now().strftime('%H:%M:%S') ~ ' | Number of rows : '~ item , True) }}
{% else %}
  {% set item = [] %}
{% endif %}

{% endmacro %}