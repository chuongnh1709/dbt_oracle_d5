/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 31-Oct-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
    materialized='view'
    , parallel=2
  ) 
}}

select 2 as c1 
FROM  {{ source('owner_int', 'in_hom_client') }}  client
where rownum < 2

