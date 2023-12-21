/*
  - created by        : chuong.nguyenh2  - vndm2 
  - created at        : 12-Dec-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_product
*/

{{ 
  config(
      materialized='incremental'
    , parallel=4 
    , unique_key='id_source'
    , merge_update_columns = ['date_effective', 'dtime_updated','flag_deleted','code_product','name_product','date_creation','name_version_status','code_product_profile']
    , tags=['sbv_dct_product','daily']
    , pre_hook="{{ dbt_log('start') }}"
    , post_hook="{{ dbt_log('end') }}"
  ) 
}}

SELECT
    ldm_sbv.s_dbt_dct_product.nextval as skp_product
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
FROM {{ ref('dbt_dct_product__map') }}

{% if is_incremental() %}
WHERE 1=1 

{% endif %}
