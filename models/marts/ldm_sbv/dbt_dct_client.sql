/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 13-Nov-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
      materialized='incremental'
    , parallel=4 
    , unique_key='id_source'
    , merge_update_columns = ['date_effective', 'dtime_updated','flag_deleted','id_cuid']
    , tags=['sbv_dct_client','daily']
    , pre_hook="insert into ldm_sbv.dbt_log (model_schema, model_name ,model_status ) values( '{{ this.schema }}', '{{ this.table }}' ,'start' )"
    , post_hook="insert into ldm_sbv.dbt_log (model_schema, model_name ,model_status) values( '{{ this.schema }}', '{{ this.table }}' ,'end' ) "
  ) 
}}

SELECT
    ldm_sbv.s_dbt_dct_client.nextval as skp_client
  , code_source_system
  , id_source
  , date_effective
  , dtime_inserted
  , dtime_updated
  , flag_deleted
  , id_cuid
FROM {{ ref('dbt_dct_client__map') }}

{% if is_incremental() %}

-- WHERE  -- or where not required
--   date_effective >=(
--       select nvl(max(date_effective), {{ var("d_def_value_date_hist") }} ) 
--       from {{ this }} 
      -- )

{% endif %}

