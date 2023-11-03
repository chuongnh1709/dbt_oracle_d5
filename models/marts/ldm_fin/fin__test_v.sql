{{ 
  config(
    materialized='view'
    , grants = {'+select': ['LDM_FIN_ETL']}
    , parallel=4 
  ) 
}}

-- sql 
select /*+ parallel (2)*/ 12 c1, sysdate c2  from dual
  where 1=1 
