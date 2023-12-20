{% docs vndw_578 %}

# VNDW-578_Migrate_SBV_DCT_PRODUCT_to_dbt_model

- As-IS : ldm_sbv_etl.lib_dct_product

- TO-BE: migrate to dbt model 

- LDM_SBV functional


    | Logical Mapping   | Model                   | Output                   |
    |-------------------|-------------------------|--------------------------|
    | Map               | dbt_dct_product__map    |                          |
    | Load              | dbt_dct_product         | ldm_sbv.dbt_dct_product  |


- File lists: 

    --03_#INCREMENTAL/000003_#PATCH_VNDW-578_Migrate_legacy_code_to_dbt_model_for_SBV_DCT_PRODUCT/001_dbt_dct_product.sql \
    --03_#INCREMENTAL/000003_#PATCH_VNDW-578_Migrate_legacy_code_to_dbt_model_for_SBV_DCT_PRODUCT/002_s_dbt_dct_product \
    --05_repeatable_grants/grant_table_to_role_vndw_578.sql \
    --legacy/lib_dct_product.sql \
    --mart/intermediate/dbt_dct_product__map.sql \
    --mart/ldm_sbv/dbt_dct_product.sql \
    --mart/ldm_sbv/dbt_dct_product.yml \
    --mart/ldm_sbv_out/dbt_dc_product.sql

- Schema for all model relate to dct_client : mart/ldm_sbv/dbt_dct_product.yml
- Build command : dbt build --model tag:sbv_dct_product 
 
{% enddocs %}