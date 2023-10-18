{{ config(materialized='ephemeral') }}
-- there will be no view or table created on database 

select * from  {{ ref('dct_supplier') }}
where supplier_category_id = 2
