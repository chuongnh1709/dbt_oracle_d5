/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 13-Nov-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
      materialized='view'
    , tags=[
      'sbv_dct_product'
    ]
    , post_hook="grant select on {{ this }} to  LDM_SBV_SELECT"
  )
}}

SELECT
    skp_product
  , code_source_system
  , id_source
  , date_effective
  , dtime_inserted
  , dtime_updated
  , flag_deleted
  , id_product
  , code_product
  , name_product
  , date_creation
  , name_version_status
  , code_product_profile
FROM {{ ref('dbt_dct_product') }}
