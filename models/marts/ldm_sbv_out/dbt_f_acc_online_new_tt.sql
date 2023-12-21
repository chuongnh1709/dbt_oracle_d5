/*
  - created by        : chuong.nguyen  - vndm2 
  - created at        : 13-Nov-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_client 
*/

{{ 
  config(
      materialized='view'
    , tags='sbv_ft_acc_online_new_tt'
    , post_hook="grant select on {{ this }} to  LDM_SBV_SELECT"
  )
}}

SELECT
  
  , code_move_type
  , amt_accounted_value
  , date_accounted
  , date_accounting_move
  , dtime_acc_system_created
  , code_transaction_subtype
  , code_credit_type
  , code_owner
  , text_contract_number
  , text_account_move_desc
  , skp_contract
  , id_contract
  , id_client
  , id_accounting_event
  , date_creation
  , dtime_inserted
  , dtime_updated
  , code_contract_term
  , name_status_acquisition
  , amt_accounted_value_r
FROM {{ ref('dbt_ft_acc_online_new_tt') }}
