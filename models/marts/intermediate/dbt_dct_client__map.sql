/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 31-Oct-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
    materialized='ephemeral'
    , parallel=2
    , tags=['sbv_dct_client','daily']
  ) 
}}

{%- set v_hom_code_source_system = 'HOM' %}

SELECT
     NULL                                                   as skp_client 
  , '{{v_hom_code_source_system}}'                          as code_source_system
  , to_char(client.id)                                      as id_source
  , {{ var("p_effective_date") }}                           as date_effective
  , sysdate                                                 as dtime_inserted
  , sysdate                                                 as dtime_updated
  , CASE
    WHEN client.code_change_type =  '{{ var("v_code_change_type_del") }}'
    THEN  '{{ var("v_flag_y") }}'
    ELSE  '{{ var("v_flag_n") }}'
    END                                                     as flag_deleted
  , nvl(client.cuid,  {{ var("n_minus_one") }}  )           as id_cuid
FROM  {{ source('owner_int', 'in_hom_client') }}  client
WHERE client.code_load_status IN ('OK', 'LOAD')
    AND client.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
    -- AND client.date_effective_inserted >= {{ var("p_effective_date") }}  -- dk nay tren D5 se ko co data 
    -- AND rownum < 10  -- for Sample data testing 
