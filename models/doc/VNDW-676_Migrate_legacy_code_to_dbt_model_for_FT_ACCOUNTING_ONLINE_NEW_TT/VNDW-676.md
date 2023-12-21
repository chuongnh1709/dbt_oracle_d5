{% docs vndw_676 %}

# VNDW-578_Migrate_SBV_FT_ACCOUNTING_ONLINE_NEW_TT_to_dbt_model

- AS-IS : ldm_sbv_etl.lib_ft_acc_online_new_tt

- TO-BE: migrate to dbt model 

- LDM_SBV functional


    | Logical Mapping   | Model                            | Output                           |
    |-------------------|----------------------------------|----------------------------------|
    | Map               | dbt_ft_acc_online_new_tt__map    |                                  |
    | Load              | dbt_ft_acc_online_new_tt         | ldm_sbv.dbt_f_acc_online_new_tt  |


- File lists: 

    --03_#INCREMENTAL/VNDW-676_Migrate_legacy_code_to_dbt_model_for_FT_ACCOUNTING_ONLINE_NEW_TT/001_dbt_ft_accounting_online_new_tt.sql  
    --03_#INCREMENTAL/VNDW-676_Migrate_legacy_code_to_dbt_model_for_FT_ACCOUNTING_ONLINE_NEW_TT/002_s_dbt_ft_accounting_online_new_tt.sql  
    --05_repeatable_grants/grant_table_to_role_vndw_676.sql  
    --legacy/lib_ft_acc_online_new.sql  
    --mart/intermediate/dbt_ft_acc_online_new_tt__map.sql  
    --mart/ldm_sbv/dbt_ft_acc_online_new_tt.sql  
    --mart/ldm_sbv/dbt_ft_acc_online_new_tt.yml  
    --mart/ldm_sbv_out/dbt_f_acc_online_new_tt.sql  

- Schema for all model relate to dbt_ft_acc_online_new_tt   : mart/ldm_sbv/dbt_ft_acc_online_new_tt.yml
- Build command                                             : dbt build --model tag:sbv_ft_acc_online_new_tt
 
{% enddocs %}