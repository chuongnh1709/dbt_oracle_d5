{{ 
  config ( alias='clt_adld_newband_test_2', enabled=true ) 
}}

select 
  *
from {{ source('ldm_fin', 'clt_adld_newband_test') }}

