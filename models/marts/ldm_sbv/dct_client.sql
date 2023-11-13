---------------------------------------------------------
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
    , tags=["daily","sbv_dct_client","load"]
    , parallel=4 
    , unique_key='id_source'
    , merge_update_columns = ['date_effective', 'skp_proc_updated','dtime_updated','flag_deleted','id_cuid']
    , on_schema_change = 'fail'
  ) 
}}

SELECT
  skp_client
  , code_source_system
  , id_source
  , date_effective
  , skp_proc_inserted
  , skp_proc_updated
  , dtime_inserted
  , dtime_updated
  , flag_deleted
  , id_cuid
FROM {{ ref('dct_client__map') }}
{% if is_incremental() %}
WHERE dtime_updated >=(select max(dtime_updated) from {{ this }} )
-- or where not required
{% endif %}
