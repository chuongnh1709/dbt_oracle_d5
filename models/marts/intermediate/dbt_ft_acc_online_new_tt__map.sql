/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 31-Oct-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
  - legacy table      : ft_acc_online_new_tt
*/

{{ 
  config(
    materialized='view'
    , parallel=8
    , tags=['sbv_ft_acc_online_new_tt','daily']
    , pre_hook=[  "{{ alter_session_parallel(8) }}" 
                , "{{ dbt_log('start') }}"
              ]
    , post_hook="{{ dbt_log('end') }}"
  ) 
}}

{%- set v_hom_code_source_system  = 'HOM' %}
{%- set v_bkng_code_source_system = 'BKNG' %}
{%- set p_effective_date_1        = 'trunc(SYSDATE-2)' %}
{%- set p_cur_date                = 'trunc(SYSDATE)' %}

WITH  bkng_rank_info AS (
    SELECT 
      id_accounting_event,
      ,MAX(CASE WHEN type = 'CONTRACT_CODE' THEN value END)          AS text_contract_number
      ,MAX(CASE WHEN type = 'TARIFF_ITEM_TYPE_CODE' THEN value END)  AS code_transaction_subtype
      ,MAX(CASE WHEN type = 'CONTRACT_TERM' THEN value END)          AS code_contract_term
      ,MAX(CASE WHEN type = 'CLIENT_SEGMENT' THEN value END)         AS name_status_acquisition
    FROM {{ source('owner_int', 'in_bkng_rank_001') }}
    WHERE type IN ('CONTRACT_CODE', 'TARIFF_ITEM_TYPE_CODE', 'CONTRACT_TERM', 'CLIENT_SEGMENT')
      AND code_load_status IN ('OK', 'LOAD')
      AND code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
      AND date_effective = {{p_effective_date_1}}
    GROUP BY id_accounting_event
    ), i AS (
    SELECT  /*+ parallel(16) */
        '{{v_bkng_code_source_system}}'      AS code_source_system
        ,to_char(b_mm.id)                    AS id_source
        ,CASE 
          WHEN b_mm.code_change_type = '{{ var("v_code_change_type_del") }}' 
          THEN '{{ var("v_flag_y") }}' 
          ELSE '{{ var("v_flag_n") }}'
        END                                  AS flag_deleted
        ,b_mm.type                           AS code_move_type
        ,b_mm.amount                         AS amt_accounted_value
        ,b_mm.accounting_date                AS date_accounted
        ,b_mm.move_date                      AS date_accounting_move
        ,b_mm.created                        AS dtime_acc_system_created
        ,bkri.code_transaction_subtype       AS code_transaction_subtype
        ,contract.code_credit_type           AS code_credit_type
        ,contract.code_credit_owner 	       AS code_owner
        ,bkri.text_contract_number           AS text_contract_number
        ,enum.value                          AS text_account_move_desc
        ,contract.skp_contract               AS skp_contract
        ,contract.id_contract                AS id_contract
        ,contract.id_client                  AS id_client
        ,b_mm.id_accounting_event            AS id_accounting_event
        ,contract.date_creation              AS date_creation
        ,bkri.code_contract_term             AS code_contract_term
        ,bkri.name_status_acquisition        AS name_status_acquisition
        ,b_mm.amount_rounded                 AS amt_accounted_value_r
    FROM {{ source('owner_int', 'in_bkng_movement_002') }} b_mm
    JOIN bkng_rank_info bkri 
        ON bkri.id_accounting_event = b_mm.id_accounting_event
    JOIN ldm_sbv.dct_contract contract  -- change to ref {{'dbt_dct_contract'}} later 
        ON bkri.text_contract_number = contract.text_contract_number
    LEFT JOIN {{ source('owner_int', 'in_csd_enum_value') }} enum 
        ON b_mm.type = enum.code 
        AND enum.enum_code = 'ACC_MOVE_TYPES'
        AND enum.enum_group_id = 'CUST'
        AND enum.code_load_status IN ('OK', 'LOAD')
        AND enum.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
        AND enum.date_effective_inserted = {{ var("p_effective_date") }} 
   WHERE
       b_mm.code_load_status IN ('OK', 'LOAD')
      AND b_mm.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
      AND b_mm.date_effective = {{ var("p_effective_date") }} 
    UNION ALL 
    SELECT  --  /*THUAN.DANGT ADD ON 2022-08-12 resolving the missing move case , due to the contract have not created at that time  */
        '{{v_bkng_code_source_system}}'          AS code_source_system
        ,to_char(b_mm.id)                        AS id_source
        ,CASE 
          WHEN b_mm.code_change_type = '{{ var("v_code_change_type_del") }}' 
          THEN '{{ var("v_flag_y") }}' 
          ELSE '{{ var("v_flag_n") }}'
        END                                      AS flag_deleted
        ,b_mm.type                               AS code_move_type
        ,b_mm.amount                             AS amt_accounted_value
        ,b_mm.accounting_date                    AS date_accounted
        ,b_mm.move_date                          AS date_accounting_move
        ,b_mm.created                            AS dtime_acc_system_created
        ,bkri.code_transaction_subtype           AS code_transaction_subtype
        ,contract.code_credit_type               AS code_credit_type
        ,contract.code_credit_owner 	           AS code_owner
        ,bkri.text_contract_number               AS text_contract_number
        ,enum.value                              AS text_account_move_desc
        ,contract.skp_contract                   AS skp_contract
        ,contract.id_contract                    AS id_contract
        ,contract.id_client                      AS id_client
        ,b_mm.id_accounting_event                AS id_accounting_event
        ,contract.date_creation                  AS date_creation
        ,bkri.code_contract_term                 AS code_contract_term
        ,bkri.name_status_acquisition            AS name_status_acquisition
        ,b_mm.amount_rounded                     AS amt_accounted_value_r
    FROM {{ source('owner_int', 'in_bkng_movement_002') }} b_mm
    JOIN bkng_rank_info bkri 
        ON bkri.id_accounting_event = b_mm.id_accounting_event
    JOIN ldm_sbv.dct_contract contract 
        ON bkri.text_contract_number = contract.text_contract_number
    LEFT JOIN {{ source('owner_int', 'in_csd_enum_value') }}  enum 
        ON b_mm.type = enum.code 
            AND enum.enum_code = 'ACC_MOVE_TYPES'
            AND enum.enum_group_id = 'CUST'
            AND enum.code_load_status IN ('OK', 'LOAD')
            AND enum.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
            AND enum.date_effective_inserted = {{p_effective_date_1}}
    WHERE
        b_mm.code_load_status IN ('OK', 'LOAD')
        AND b_mm.code_change_type IN ('X', 'I', 'U', 'D', 'M', 'N')
        AND b_mm.date_effective = {{p_effective_date_1}}
        AND contract.date_effective  = {{ var("p_effective_date") }} 
        AND contract.dtime_inserted > {{p_cur_date}}
        AND NOT EXISTS (
            SELECT 1
            FROM ldm_sbv.ft_accounting_online_new_tt tt -- change to {{ ref('dbt_ft_accounting_online_new_tt') }}
            WHERE tt.id_source = to_char(b_mm.id)
        )
        -- AND NOT EXISTS (
        --     SELECT 1
        --     FROM ldm_sbv.stm_accounting_online_new_tt stm
        --     WHERE stm.id_source = to_char(b_mm.id)
        -- )
    )
    SELECT distinct
       NULL AS skf_accounting_online,
      ,i.code_source_system                                                 AS code_source_system
      ,i.id_source                                                          AS id_source
      ,{{ var("p_effective_date") }}                                        AS date_effective
      ,i.flag_deleted                                                       AS flag_deleted
      ,nvl(i.code_move_type, '{{ var("v_xna") }}' )                         AS code_move_type
      ,i.amt_accounted_value                                                AS amt_accounted_value
      ,nvl(i.date_accounted,{{ var("d_def_value_date_hist") }} )            AS date_accounted
      ,nvl(i.date_accounting_move,{{ var("d_def_value_date_hist") }} )      AS date_accounting_move
      ,nvl(i.dtime_acc_system_created, {{ var("d_def_value_date_hist") }} ) AS dtime_acc_system_created
      ,nvl(i.code_transaction_subtype, '{{ var("v_xna") }}')                AS code_transaction_subtype
      ,i.code_credit_type                                                   AS code_credit_type
      ,nvl(i.code_owner, '{{ var("v_xna") }}')                              AS code_owner
      ,nvl(i.text_contract_number, '{{ var("v_xna") }}')                    AS text_contract_number
      ,nvl(i.text_account_move_desc, '{{ var("v_xna") }}')                  AS text_account_move_desc
      ,i.skp_contract                                                       AS skp_contract
      ,i.id_contract                                                        AS id_contract
      ,i.id_client                                                          AS id_client
      ,i.id_accounting_event                                                AS id_accounting_event
      ,i.date_creation                                                      AS date_creation
      ,sysdate                                                              AS dtime_inserted
      ,sysdate                                                              AS dtime_updated
      ,i.code_contract_term                                                 AS code_contract_term
      ,nvl(i.name_status_acquisition, '{{ var("v_xna") }}')                 AS name_status_acquisition
      ,i.amt_accounted_value_r
    FROM i
