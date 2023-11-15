/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 13-Nov-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

--- config for dct_client 
{{ 
  config(
      materialized='incremental'
    , parallel=4 
    , unique_key='id_source'
    , merge_update_columns = ['date_effective', 'dtime_updated','flag_deleted','id_cuid']
  ) 
}}

SELECT
    ldm_fin.s_dct_client.nextval as skp_client
  , code_source_system
  , id_source
  , date_effective
  , dtime_inserted
  , dtime_updated
  , flag_deleted
  , id_cuid
FROM {{ ref('dct_client__map') }}
{% if is_incremental() %}
WHERE date_effective >=(
      select nvl(max(date_effective), {{ var("d_def_value_date_hist") }} ) 
      from {{ this }} 
      )
-- or where not required
{% endif %}

