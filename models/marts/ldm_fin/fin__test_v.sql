{{ 
  config(
    materialized='view'
    , grants = {'+select': ['LDM_CBE_ETL']}
    , parallel=4 
  ) 
}}

-- sql 
select /*+ parallel (2)*/ 12 c1, sysdate c2  
 , {{ var("p_effective_date") }}  as last_day
from dual
  where 1=1 
