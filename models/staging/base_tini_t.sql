{{ config(  materialized = 'incremental') }}

select * from `learn-dbt-397522.dbt_bigquery.tiny_t` -- source 
