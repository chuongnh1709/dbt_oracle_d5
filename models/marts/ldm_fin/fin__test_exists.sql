{{ 
  config(
      materialized='view'
  ) 
}}

{% set n_101 = 101 %}

-- model query  
--   select 
--       {{ n_101 }} c1
--     , sysdate c2  
--     , {{ var('p_effective_date') }}  as last_day
--   from dual
  
-- {% if is_incremental() %}
--   where 1=1
-- {% endif %}

-- union all 

{{ fn_verifica_id(2) }}
