/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 31-Oct-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
      materialized='view'
    , pre_hook="{{ alter_session_parallel(4) }}"
  ) 
}}

select 2 as c1 
from dual

