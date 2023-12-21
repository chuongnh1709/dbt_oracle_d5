/*
  - created by        : chuong.nguyenh2  - vndm2 
  - created at        : 12-Dec-2023
  - updated at        : 01-Jan-3000
  - main model/table  : ldm_sbv.dct_product
*/

{{ 
  config(
      materialized='incremental'
    , parallel=4 
    , unique_key='id_source'
    , merge_update_columns = [
        'date_effective', 'dtime_updated','flag_deleted','code_move_type','amt_accounted_value','date_creation','date_accounted','date_accounting_move'
        ,'dtime_acc_system_created','code_transaction_subtype','code_credit_type','code_owner','text_account_move_desc','id_contract','id_client','id_accounting_event'
        ,'code_contract_term','name_status_acquisition','amt_accounted_value_r'
      ]
    , tags=[
        'sbv_ft_acc_online_new_tt','daily'
        ]
    , pre_hook="{{ dbt_log('start') }}"
    , post_hook="{{ dbt_log('end') }}"
  ) 
}}

SELECT
    ldm_sbv.s_dbt_ft_accounting_online_new_tt.nextval as skf_accounting_online
  , code_source_system
  , id_source
  , date_effective
  , dtime_inserted
  , dtime_updated
  , flag_deleted
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
  , code_contract_term
  , name_status_acquisition
  , amt_accounted_value_r
FROM {{ ref('dbt_ft_acc_online_new_tt__map') }}

{% if is_incremental() %}
WHERE 1=1 

{% endif %}
