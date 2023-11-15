{% macro fn_verifica_id(id=100) %}

{% if execute %}

  {% set query %}
      select 1 as resultado from dual 
      union all 
      select 2 as resultado from dual 
  {% endset %}

  {% set resultado = run_query(query) %}

  -- get data from specific row of column 1 
  {% set item_col_1 = resultado.columns[0].values() %}  -- get column 1 --> return tuple  (1, 2)
  
  {% set item = resultado.columns[0].values()[0] %}  -- get column 1, row 1 
  {% set item = resultado.columns[0].values()[1] %}  -- get column 1, row 2 

  {% if item == 2 %}  -- 2==2
      {{ log('[YES] ' ~ item ~ '-' ~ item_col_1 , info=True) }}

  {% else %}
      {{ log('[NO] ' ~ item , info=True) }}
  {% endif %}

{% endif %}

-- iterate all row for column 1 
{{ log('[For loop ] ' , info=True) }}
{% for i in item_col_1 %}
  {% if i == 2 %}  
      {{ log('[YES] ' ~ i ~ '-' ~ item_col_1 , info=True) }}

  {% else %}
      {{ log('[NO] ' ~ i ~ '-' ~ item_col_1  , info=True) }}
  {% endif %}

{% endfor %}



  select 
      999     c1
    , sysdate c2  
    , sysdate last_day
  from dual    -- dummy SQL for parsing stage

{% endmacro %}