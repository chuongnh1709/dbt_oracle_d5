-- Incremental Model : `learn-dbt-397522.dbt_bigquery_staging.stg_tini_t`

{{ config(
    materialized = 'incremental',
    unique_key = 'id',
    merge_update_columns = ['status'],
    schema_change = 'append_new_columns' 
) }}

select * from `learn-dbt-397522.dbt_bigquery.tiny_t` -- source 

{% if is_incremental() %}

where
  updated_at > (select max(updated_at) from {{ this }})

{% endif %}
