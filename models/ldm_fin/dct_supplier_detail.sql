-- https://www.youtube.com/watch?v=Lw8K9BML6jQ

{{ config(materialized='view') }}

with supplier as 
(
  select 
      supplier_id
    , supplier_name
    , supplier_category_id
    , phone_number
  from  {{ ref('dct_supplier_ephe') }} -- ephemeral as CTE
)
, product as (
  select 
      supplier_id
    , product_id 
    , product_name 
    , brand_name
    , product_size
    , typical_weight_per_unit
  from   {{ ref('dct_product') }} as product
)
select p.* 
from product p 
join supplier s
  on p.supplier_id = s.supplier_id

-- dbt run -m +dct_supplier_detail
-- check dataset : dbt_bigquery_fin 