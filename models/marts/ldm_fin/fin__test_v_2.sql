{{ 
  config(
    materialized='view'
  ) 
}}

/*{{ _log("[INFO] Building : " ~ this , info=True ) }}*/

-- sql 
  select 
    99 c1
  , sysdate c2  
  , {{ var("p_effective_date") }}  as last_day
from dual


