{{ 
  config(
    materialized='view'
    , grants = {'+select': ['LDM_CBE_ETL']}
    , parallel=4 
  ) 
}}

{% set model_name =   'fin__test' %}
{{dbt_utils.log_info('Start build fin__test_v model as ' ~ model_name ) }}

{{ log("[INFO] Building : " ~ model_name , info=True ) }}

-- sql 
with base as (
  select /*+ parallel (2)*/ 
      12 c1
    , sysdate c2  
    , {{ var("p_effective_date") }}  as last_day
  from dual

  union all 
  select c1, c2, last_day from  {{ ref('fin__test') }}

  union all 
  select c1, c2, last_day from  {{ ref('fin__test_ephe') }}
)

SELECT *
FROM base