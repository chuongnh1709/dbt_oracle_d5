/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 31-Nov-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
    materialized='table'
    , tags=["dct_client","mapping"]
    , pre_hook="{{ truncate_table('ldm_sbv', this.table) }}"
    , parallel=4 
  ) 
}}

---------------------------------------------------------
-- -- WF : LDM_SBV_ETL.LIB_DCT_CLIENT
    -- dim_client_map_sbv  -> dim_client_load_sbv -> dim_client_late_map_sbv -> dim_client_load_sbv
---------------------------------------------------------
-- Map 
-- combine dim_client_map_sbv and dim_client_late_map_sbv
SELECT
  NULL                                                    AS skp_client,
  v_hom_code_source_system                                AS code_source_system,
  i.id_source                                             AS id_source,
  p_effective_date                                        AS date_effective,
  p_process_key                                           AS skp_proc_inserted,
  p_process_key                                           AS skp_proc_updated,
  sysdate                                                 AS dtime_inserted,
  sysdate                                                 AS dtime_updated,
  CASE
    WHEN i.code_change_type = v_code_change_type_del
    THEN v_flag_y
    ELSE v_flag_n END                                     AS flag_deleted,
  nvl(i.cuid,  n_minus_one )                              AS id_cuid
FROM(
    SELECT
      to_char(client.id)                                  AS id_source,
      client.cuid    ,
      client.code_change_type
    FROM owner_int.in_hom_client client
    WHERE client.code_load_status IN ('OK', 'LOAD')
        AND client.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
        AND client.date_effective_inserted >= '{{ var("p_effective_date") }}' 
      ) i
