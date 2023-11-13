-- config 
{{ 
  config(
    materialized='incremental'
    , unique_key='c1'
    , tags=["test", "ldm_fin"]
    , parallel=4 
    , pre_hook="{{ truncate_table('ldm_fin', this.table) }}"
  ) 
}}

-- model query  
select /*+ parallel (2)*/ 
    99 c1
  , sysdate c2  
  , {{ var("p_effective_date") }}  as last_day
from dual

{% if is_incremental() %}
  where 1=1
{% endif %}



