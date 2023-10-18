-- Model name : base_cl_bi_pcs_payment_channel
-- This model is replicate 1:1 from Source 

select 
    skp_payment_channel
  , text_payment_channel
  , code_payment
  , flag_active
from {{ source('ldm_fin_source', 'clt_bi_pcs_payment_channel') }}  --source
