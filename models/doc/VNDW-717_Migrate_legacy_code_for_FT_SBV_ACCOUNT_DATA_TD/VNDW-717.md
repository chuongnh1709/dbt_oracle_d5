{% docs vndw_717 %}

# VNDW-717 Migrate legacy code to dbt model for SBV FT_SBV_ACCOUNT_DATA_TD

- AS-IS : ldm_sbv_etl.lib_ft_acc_online_new_tt

- TO-BE: migrate to dbt model 

- LDM_SBV functional


    | Logical Mapping   | Model                              | Output                             |
    |-------------------|------------------------------------|------------------------------------|
    | Map               | dbt_ft_sbv_account_data_td__map    |                                    |
    | Load              | dbt_ft_sbv_account_data_td         | ldm_sbv.dbt_f_sbv_account_data_td  |


- File lists: 

    --03_#INCREMENTAL/VNDW-717  
    --03_#INCREMENTAL/VNDW-717  
    --05_repeatable_grants/grant_table_to_role_vndw_717.sql  
    --legacy/lib_ft_sbv_account_td.pck  

    --mart/intermediate/dbt_xxx__map.sql  
    --mart/ldm_sbv/dbt_xxx.sql  
    --mart/ldm_sbv/dbt_xxx.yml  
    --mart/ldm_sbv_out/xxx.sql  

- Schema for all model relate to dbt_ft_acc_online_new_tt   : mart/ldm_sbv/dbt_ft_acc_online_new_tt.yml
- Build command                                             : dbt build --model tag:sbv_ft_acc_online_new_tt
 
{% enddocs %}