{{ 
  config(
    materialized='view'
    , grants = {'+select': ['LDM_FIN_ETL']}
    , parallel=4 
  ) 
}}

-- sql 
select /*+ parallel (2)*/ 123 c1, sysdate c2  from dual

{% if is_incremental() %}
  where 1=1
{% endif %}
