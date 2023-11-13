/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 31-Oct-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
    materialized='ephemeral'
    , tags=["daily","sbv_dct_client","map"]
    , parallel=4 
  ) 
}}

/*
  -- WF : LDM_SBV_ETL.LIB_DCT_CLIENT
  -- dim_client_map_sbv  -> dim_client_load_sbv -> dim_client_late_map_sbv -> dim_client_load_sbv
-- Map 
-- combine dim_client_map_sbv and dim_client_late_map_sbv
--{{ truncate_table('ldm_sbv', this.table) }}
*/

SELECT
    ldm_sbv.s_dct_client.nextval                            AS skp_client
  , v_hom_code_source_system                                AS code_source_system
  , to_char(client.id)                                      AS id_source
  , p_effective_date                                        AS date_effective
  , sysdate                                                 AS dtime_inserted
  , sysdate                                                 AS dtime_updated
  , CASE
    WHEN client.code_change_type = v_code_change_type_del
    THEN v_flag_y
    ELSE v_flag_n 
    END                                                     AS flag_deleted
  , nvl(client.cuid,  n_minus_one )                         AS id_cuid
FROM  {{ source('owner_int', 'in_hom_client') }}  client
WHERE client.code_load_status IN ('OK', 'LOAD')
    AND client.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
    AND client.date_effective_inserted >= {{ var("p_effective_date") }}
