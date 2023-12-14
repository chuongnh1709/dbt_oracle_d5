/*
  - created by        : chuong.nguyenh2  - vndm2 
  - created at        : 12-Dec-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_product

  - WF : LDM_SBV_ETL.LIB_DCT_PRODUCT
    DIM_DCT_PRODUCT_MAP_SBV  -> DIM_DCT_PRODUCT_LOAD_SBV -> DIM_DCT_PRODUCT_MAP_LATE_SBV -> DIM_DCT_PRODUCT_LOAD_SBV_2
  
  - DBT Migrate code : -- DIM_DCT_PRODUCT_MAP_SBV + DIM_DCT_PRODUCT_MAP_LATE_SBV --> LDM_SBV.STM_PRODUCT
*/

{{ 
  config(
    materialized='ephemeral'
    , parallel=2
    , tags=['sbv_dct_product','daily']
    , pre_hook="{{ dbt_log('start') }}"
    , post_hook="{{ dbt_log('end') }}"
  ) 
}}

{%- set v_hom_code_source_system = 'HOM' %}

{#
/* -- ephemeral not support WITH clause 
WITH int_hom_product AS (
  SELECT
        to_char(product.id) AS id_source
      , product.id
      , product.code_change_type
      , product.code
      , product.name
      , product.creation_date
      , product.version_status
      , product.product_profile_code
  FROM  {{ source('owner_int', 'in_hom_product') }}  product
  WHERE	product.code_load_status IN ('OK', 'LOAD')
      AND product.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
      AND product.date_effective_inserted >= {{ var("p_effective_date") }}
  )
  SELECT
        NULL                                                                AS skp_product
      , '{{v_hom_code_source_system}}'                                      AS code_source_system
      , i.id_source                                                         AS id_source
      , {{ var("p_effective_date") }}                                       AS date_effective
      , sysdate                                                             AS dtime_inserted
      , sysdate                                                             AS dtime_updated
      , CASE
          WHEN i.code_change_type = v_code_change_type_del THEN '{{ var("v_flag_y") }}'
        ELSE '{{ var("v_flag_n") }}' END                                    AS flag_deleted
      , i.id                                                                AS id_product
      , nvl(i.code, '{{ var("v_xna") }}' )                                  AS code_product
      , nvl(i.name, '{{ var("v_xna") }}' )                                  AS name_product
      , nvl(i.creation_date, {{ var("d_def_value_date_future") }}  )        AS date_creation
      , i.version_status                                                    AS name_version_status
      , i.product_profile_code                                              AS code_product_profile
  FROM int_hom_product i
*/ 
#}

SELECT
      NULL                                                                AS skp_product
    , '{{v_hom_code_source_system}}'                                      AS code_source_system
    , i.id_source                                                         AS id_source
    , {{ var("p_effective_date") }}                                       AS date_effective
    , sysdate                                                             AS dtime_inserted
    , sysdate                                                             AS dtime_updated
    , CASE
        WHEN i.code_change_type = '{{ var("v_code_change_type_del") }}'
        THEN '{{ var("v_flag_y") }}'
        ELSE '{{ var("v_flag_n") }}' 
      END                                    AS flag_deleted
    , i.id                                                                AS id_product
    , nvl(i.code, '{{ var("v_xna") }}' )                                  AS code_product
    , nvl(i.name, '{{ var("v_xna") }}' )                                  AS name_product
    , nvl(i.creation_date, {{ var("d_def_value_date_future") }}  )        AS date_creation
    , i.version_status                                                    AS name_version_status
    , i.product_profile_code                                              AS code_product_profile
FROM 
  (
    SELECT
      to_char(product.id) AS id_source
    , product.id
    , product.code_change_type
    , product.code
    , product.name
    , product.creation_date
    , product.version_status
    , product.product_profile_code
FROM  {{ source('owner_int', 'in_hom_product') }}  product
WHERE	product.code_load_status IN ('OK', 'LOAD')
    AND product.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
--    AND product.date_effective_inserted >= {{ var("p_effective_date") }}  -- comment for run on D5
  ) i