{% docs vndw_559 %}

# VNDW-559_Migrate_SBV_DCT_CLIENT_to_dbt_model

- AS-IS : ldm_sbv_etl.lib_dct_client

- TO-BE: migrate to dbt model 

- LDM_SBV functional

    | Logical Mapping   | Model                  | Output                  |
    |-------------------|------------------------|-------------------------|
    | Map               | dbt_dct_client__map    |                         |
    | Load              | dbt_dct_client         | ldm_sbv.dbt_dct_client  |

- File lists: 

    --03_#INCREMENTAL/000002_#PATCH_VNDW-559_Migrate_legacy_code_to_dbt_model_for_SBV_DCT_CLIENT/001_dbt_dct_client.sql \
    --03_#INCREMENTAL/000002_#PATCH_VNDW-559_Migrate_legacy_code_to_dbt_model_for_SBV_DCT_CLIENT/002_s_dbt_dct_client.sql \
    --05_repeatable_grants/grant_table_to_role_vndw_559.sql \
    --legacy/lib_dct_client.sql \
    --mart/intermediate/dbt_dct_client__map.sql \
    --mart/ldm_sbv/dbt_dct_client.sql \
    --mart/ldm_sbv_out/dbt_dc_client.sql

- Schema for all model relate to dct_client : mart/ldm_sbv/dbt_dct_client.yml
- Build command : dbt build --model tag:sbv_dct_client 
 
{% enddocs %}