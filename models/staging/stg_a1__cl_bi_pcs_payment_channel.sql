-- Model name : stg_a1__cl_bi_pcs_payment_channel
-- This model transform data from Base layer into Staging layer 

with source as (
  select 
      skp_payment_channel
    , text_payment_channel
    , code_payment
    , flag_active
  from {{ source('ldm_fin_source', 'clt_bi_pcs_payment_channel') }}  --source
)
select 
   s.* 
  , case 
      when s.flag_active = 1 then 'Active'
      else '{{ var("v_xna") }}'
    end as code_active
from source s 
