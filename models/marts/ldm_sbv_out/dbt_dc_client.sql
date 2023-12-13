/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 13-Nov-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
      materialized='view'
    , tags=['sbv_dct_client']
    , pre_hook="insert into ldm_sbv.dbt_log (model_schema, model_name ,model_status ) values( '{{ this.schema }}', '{{ this.table }}' ,'start' )"
    , post_hook=[
        "insert into ldm_sbv.dbt_log (model_schema, model_name ,model_status) values( '{{ this.schema }}', '{{ this.table }}' ,'end' ) "
        , "grant select on {{ this }} to  LDM_SBV_SELECT"
      ]
  ) 
}}


SELECT
    skp_client
  , code_source_system
  , id_source
  , date_effective
  , dtime_inserted
  , dtime_updated
  , flag_deleted
  , id_cuid
  -- , aaa -- this will issue database error 
FROM {{ ref('dbt_dct_client') }}
