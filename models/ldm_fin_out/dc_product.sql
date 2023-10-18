{{ config(materialized='view') }}

select 
   product_name  as product_name 
  , brand_name
  , product_size
  , typical_weight_per_unit
from {{ref ('dct_product') }}  product 