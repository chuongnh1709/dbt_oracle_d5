{{ 
  config(
    materialized='view'
    , grants = {'+select': ['LDM_CBE_ETL']}
    , parallel=4 
  ) 
}}

{{ log("[INFO] Building : " ~ this , info=True ) }}

-- sql 
  select c1, c2, last_day from  {{ ref('fin__test') }}



