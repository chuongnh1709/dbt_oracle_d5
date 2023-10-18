{{ config(materialized='table') }}

select
    supplier_id                 as supplier_id
  , upper(supplier_name)        as supplier_name
  , supplier_category_id        as supplier_category_id
  , supplier.phone_number       as phone_number
from  `vit-lam-data.wide_world_importers.purchasing__suppliers` as supplier